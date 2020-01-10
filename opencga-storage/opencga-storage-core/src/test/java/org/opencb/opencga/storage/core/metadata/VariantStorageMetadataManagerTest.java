package org.opencb.opencga.storage.core.metadata;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class VariantStorageMetadataManagerTest extends VariantStorageBaseTest implements VariantStorageTest {

    private VariantStorageMetadataManager metadataManager;

    @Before
    public void setUp() throws Exception {
        metadataManager = getVariantStorageEngine().getMetadataManager();
    }

    @Test
    public void testTask() throws StorageEngineException {
        StudyMetadata study = metadataManager.createStudy("study");
        int id = metadataManager.registerFile(study.getId(), "file.txt");
        TaskMetadata myTask1 = metadataManager.addRunningTask(study.getId(), "MyTask1", Collections.singletonList(id), true, TaskMetadata.Type.OTHER, t -> true);
        TaskMetadata myTask2 = metadataManager.addRunningTask(study.getId(), "MyTask2", Collections.singletonList(id), true, TaskMetadata.Type.OTHER, t -> true);

        Assert.assertEquals("RUNNING", Arrays.asList("MyTask1", "MyTask2"), getTasks(study, Collections.singletonList(TaskMetadata.Status.RUNNING)));
        Assert.assertEquals("READY", Collections.emptyList(), getTasks(study, Collections.singletonList(TaskMetadata.Status.READY)));

        metadataManager.updateTask(study.getId(), myTask1.getId(), t -> t.addStatus(TaskMetadata.Status.READY));
        metadataManager.addRunningTask(study.getId(), "MyTask3", Collections.singletonList(id), true, TaskMetadata.Type.OTHER, t -> true);
        metadataManager.updateTask(study.getId(), myTask2.getId(), t -> t.addStatus(TaskMetadata.Status.DONE));

        Assert.assertEquals("RUNNING", Collections.singletonList("MyTask3"), getTasks(study, Collections.singletonList(TaskMetadata.Status.RUNNING)));
        Assert.assertEquals("READY", Collections.singletonList("MyTask1"), getTasks(study, Collections.singletonList(TaskMetadata.Status.READY)));
        Assert.assertEquals("DONE", Collections.singletonList("MyTask2"), getTasks(study, Collections.singletonList(TaskMetadata.Status.DONE)));
    }

    public List<String> getTasks(StudyMetadata study, List<TaskMetadata.Status> status) {
        return Arrays.stream(Iterators.toArray(metadataManager.taskIterator(study.getId(), status), TaskMetadata.class))
                .map(TaskMetadata::getName)
                .collect(Collectors.toList());
    }
}