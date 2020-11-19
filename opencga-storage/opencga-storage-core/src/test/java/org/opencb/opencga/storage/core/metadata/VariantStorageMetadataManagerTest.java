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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class VariantStorageMetadataManagerTest extends VariantStorageBaseTest implements VariantStorageTest {

    private VariantStorageMetadataManager metadataManager;

    @Before
    public void setUp() throws Exception {
        metadataManager = getVariantStorageEngine().getMetadataManager();
    }

    @Test
    public void testGetId() throws StorageEngineException {
        StudyMetadata study = metadataManager.createStudy("study");
        StudyMetadata study2 = metadataManager.createStudy("study2");
        int fileId = metadataManager.registerFile(study.getId(), "file.txt", Arrays.asList("s1", "s2"));

        Integer actualSampleId = metadataManager.getSampleId(study.getId(), "s1");
        Assert.assertEquals(actualSampleId, metadataManager.getSampleId(study.getId(), study.getName() + ":s1"));
        Assert.assertEquals(actualSampleId, metadataManager.getSampleId(study.getId(), actualSampleId));

        Assert.assertNull(metadataManager.getSampleId(study.getId(), String.valueOf(actualSampleId)));
        Assert.assertNull(metadataManager.getSampleId(study2.getId(), actualSampleId));
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

    @Test
    public void registerFileParallel() throws Exception {
        StudyMetadata study = metadataManager.createStudy("study");
        ExecutorService service = Executors.newFixedThreadPool(10);

        int numSamples = 3000;
        List<String> sampleNames = IntStream.range(0, numSamples).mapToObj(i -> "SAMPLE_" + i).collect(Collectors.toList());
        int numFiles = 30;

        for (int i = 0; i < numFiles; i++) {
            String fileName = "file." + i + ".txt";
            service.submit(() -> metadataManager.registerFile(study.getId(), fileName, sampleNames));
        }

        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.MINUTES));

        AtomicInteger totalNumFiles = new AtomicInteger();
        metadataManager.fileMetadataIterator(study.getId()).forEachRemaining(fileMetadata -> {
            Assert.assertEquals(fileMetadata.getName(), numSamples, fileMetadata.getSamples().size());
            totalNumFiles.getAndIncrement();
        });
        assertEquals(numFiles, totalNumFiles.get());

        AtomicInteger totalNumSamples = new AtomicInteger();
        metadataManager.sampleMetadataIterator(study.getId()).forEachRemaining(sampleMetadata -> {
            Assert.assertEquals(sampleMetadata.getName(), numFiles, sampleMetadata.getFiles().size());
            totalNumSamples.getAndIncrement();
        });
        assertEquals(numSamples, totalNumSamples.get());

    }

    public List<String> getTasks(StudyMetadata study, List<TaskMetadata.Status> status) {
        return Arrays.stream(Iterators.toArray(metadataManager.taskIterator(study.getId(), status), TaskMetadata.class))
                .map(TaskMetadata::getName)
                .collect(Collectors.toList());
    }
}