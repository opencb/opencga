package org.opencb.opencga.storage.core.metadata;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;

import java.util.ArrayList;
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

public abstract class VariantStorageMetadataManagerTest {

    private VariantStorageMetadataManager metadataManager;

    @Before
    public void setUp() throws Exception {
        metadataManager = getMetadataManager();
    }

    protected abstract VariantStorageMetadataManager getMetadataManager() throws Exception;

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

    @Test
    public void testRegisterFilesSameName() throws StorageEngineException {
        String fileName = "my.vcf";
        String path1 = "path/to/" + fileName;
        String path2 = "path/to/another/" + fileName;
        String path3 = "path/to/another/other/" + fileName;
        String path4 = "path/to/another/other/other/" + fileName;

        StudyMetadata study = metadataManager.createStudy("study");
        int fileId1 = metadataManager.registerFile(study.getId(), path1, Collections.emptyList());
        metadataManager.addIndexedFiles(study.getId(), Collections.singletonList(fileId1));
        System.out.println("fileId1 = " + fileId1);

        int fileId2 = metadataManager.registerFile(study.getId(), path2, Collections.emptyList());
        metadataManager.addIndexedFiles(study.getId(), Collections.singletonList(fileId2));
        System.out.println("fileId2 = " + fileId2);

        int fileId3 = metadataManager.registerFile(study.getId(), path3, Collections.emptyList());
        metadataManager.addIndexedFiles(study.getId(), Collections.singletonList(fileId3));
        System.out.println("fileId3 = " + fileId3);

        int fileId4 = metadataManager.registerFile(study.getId(), path4, Collections.emptyList());
        metadataManager.addIndexedFiles(study.getId(), Collections.singletonList(fileId4));
        System.out.println("fileId4 = " + fileId4);
    }

    public List<String> getTasks(StudyMetadata study, List<TaskMetadata.Status> status) {
        return Arrays.stream(Iterators.toArray(metadataManager.taskIterator(study.getId(), status), TaskMetadata.class))
                .map(TaskMetadata::getName)
                .collect(Collectors.toList());
    }

    @Test
    public void testAddSampleToCohort() throws Exception {
        StudyMetadata study = metadataManager.createStudy("study");

        metadataManager.registerCohort(study.getName(), "cohort1", Collections.emptyList());

        int numSamples = 100;
        List<Integer> sampleIds = new ArrayList<>(numSamples);
        for (int i = 0; i < numSamples; i++) {
            sampleIds.add(metadataManager.registerSample(study.getId(), null, "sample_" + i));
        }

        metadataManager.addSamplesToCohort(study.getId(), "cohort1", sampleIds.subList(0, 10));
        VariantStorageMetadataManager metadataManager = Mockito.spy(this.metadataManager);
        metadataManager.addSamplesToCohort(study.getId(), "cohort1", sampleIds.subList(0, 11));
        Mockito.verify(metadataManager, Mockito.times(1)).updateSampleMetadata(Mockito.anyInt(), Mockito.anyInt(), Mockito.any());

        Mockito.reset(metadataManager);
        metadataManager.addSamplesToCohort(study.getId(), "cohort1", sampleIds.subList(0, 11));
        Mockito.verify(metadataManager, Mockito.never()).updateSampleMetadata(Mockito.anyInt(), Mockito.anyInt(), Mockito.any());
        metadataManager.setSamplesToCohort(study.getId(), "cohort1", sampleIds.subList(0, 11));
        Mockito.verify(metadataManager, Mockito.never()).updateSampleMetadata(Mockito.anyInt(), Mockito.anyInt(), Mockito.any());

        metadataManager.setSamplesToCohort(study.getId(), "cohort1", sampleIds.subList(0, 12));
        Mockito.verify(metadataManager, Mockito.times(1)).updateSampleMetadata(Mockito.anyInt(), Mockito.anyInt(), Mockito.any());

        Mockito.reset(metadataManager);
        metadataManager.setSamplesToCohort(study.getId(), "cohort1", sampleIds.subList(0, 6));
        Mockito.verify(metadataManager, Mockito.times(6)).updateSampleMetadata(Mockito.anyInt(), Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void testRemoveSample() throws StorageEngineException {
        StudyMetadata study = metadataManager.createStudy("study");
        int fileId = metadataManager.registerFile(study.getId(), "file.txt", Arrays.asList("s1", "s2"));
        metadataManager.addIndexedFiles(study.getId(), Arrays.asList(fileId));
        int sampleId = metadataManager.getSampleId(study.getId(), "s1");
        int cohortId = metadataManager.registerCohort(study.getName(), "ALL", Arrays.asList("s1", "s2"));
        metadataManager.updateCohortMetadata(study.getId(), cohortId, cm -> {
            cm.setStatsStatus(TaskMetadata.Status.READY);
        });

        Assert.assertEquals(2, metadataManager.getFileMetadata(study.getId(), fileId).getSamples().size());
        Assert.assertEquals(1, metadataManager.getSampleMetadata(study.getId(), sampleId).getFiles().size());
        Assert.assertTrue(metadataManager.getCohortMetadata(study.getId(), cohortId).isStatsReady());

        metadataManager.removeSamples(study.getId(), Collections.singletonList(sampleId));

        Assert.assertEquals(1, metadataManager.getFileMetadata(study.getId(), fileId).getSamples().size());
        Assert.assertEquals(0, metadataManager.getSampleMetadata(study.getId(), sampleId).getFiles().size());
        Assert.assertFalse(metadataManager.getCohortMetadata(study.getId(), cohortId).isStatsReady());
    }

    @Test
    public void testRemoveSampleMultiFile() throws StorageEngineException {
        StudyMetadata study = metadataManager.createStudy("study");
        int fileId = metadataManager.registerFile(study.getId(), "file.txt", Arrays.asList("s1", "s2"));
        int fileId2 = metadataManager.registerFile(study.getId(), "file2.txt", Arrays.asList("s1", "s2"));
        metadataManager.addIndexedFiles(study.getId(), Arrays.asList(fileId, fileId2));
        int sampleId = metadataManager.getSampleId(study.getId(), "s1");
        int cohortId = metadataManager.registerCohort(study.getName(), "ALL", Arrays.asList("s1", "s2"));
        metadataManager.updateCohortMetadata(study.getId(), cohortId, cm -> {
            cm.setStatsStatus(TaskMetadata.Status.READY);
        });

        Assert.assertEquals(2, metadataManager.getFileMetadata(study.getId(), fileId).getSamples().size());
        Assert.assertEquals(2, metadataManager.getSampleMetadata(study.getId(), sampleId).getFiles().size());
        Assert.assertTrue(metadataManager.getCohortMetadata(study.getId(), cohortId).isStatsReady());

        metadataManager.removeSamples(study.getId(), Collections.singletonList(sampleId));

        Assert.assertEquals(1, metadataManager.getFileMetadata(study.getId(), fileId).getSamples().size());
        Assert.assertEquals(0, metadataManager.getSampleMetadata(study.getId(), sampleId).getFiles().size());
        Assert.assertFalse(metadataManager.getCohortMetadata(study.getId(), cohortId).isStatsReady());
    }

    @Test
    public void testRemoveFile() throws StorageEngineException {
        StudyMetadata study = metadataManager.createStudy("study");
        int fileId = metadataManager.registerFile(study.getId(), "file.txt", Arrays.asList("s1", "s2"));
        int fileId2 = metadataManager.registerFile(study.getId(), "file2.txt", Arrays.asList("s3", "s4"));
        metadataManager.addIndexedFiles(study.getId(), Arrays.asList(fileId, fileId2));
        int sampleId1 = metadataManager.getSampleId(study.getId(), "s1");
        int sampleId3 = metadataManager.getSampleId(study.getId(), "s3");
        int cohortId = metadataManager.registerCohort(study.getName(), "ALL", Arrays.asList("s1", "s2", "s3", "s4"));
        metadataManager.updateCohortMetadata(study.getId(), cohortId, cm -> {
            cm.setStatsStatus(TaskMetadata.Status.READY);
        });

        Assert.assertEquals(2, metadataManager.getFileMetadata(study.getId(), fileId).getSamples().size());
        Assert.assertEquals(2, metadataManager.getFileMetadata(study.getId(), fileId2).getSamples().size());
        Assert.assertEquals(1, metadataManager.getSampleMetadata(study.getId(), sampleId1).getFiles().size());
        Assert.assertEquals(1, metadataManager.getSampleMetadata(study.getId(), sampleId3).getFiles().size());
        Assert.assertTrue(metadataManager.getCohortMetadata(study.getId(), cohortId).isStatsReady());
        Assert.assertEquals(4, metadataManager.getCohortMetadata(study.getId(), cohortId).getSamples().size());

        metadataManager.removeIndexedFiles(study.getId(), Collections.singletonList(fileId2));

        Assert.assertEquals(2, metadataManager.getFileMetadata(study.getId(), fileId).getSamples().size());
        Assert.assertEquals(1, metadataManager.getSampleMetadata(study.getId(), sampleId1).getFiles().size());
        Assert.assertEquals(0, metadataManager.getSampleMetadata(study.getId(), sampleId3).getFiles().size());
        Assert.assertTrue(metadataManager.getCohortMetadata(study.getId(), cohortId).isInvalid());
        Assert.assertEquals(2, metadataManager.getCohortMetadata(study.getId(), cohortId).getSamples().size());
    }

    @Test
    public void testRemoveFileSampleMultiFile() throws StorageEngineException {
        StudyMetadata study = metadataManager.createStudy("study");
        int fileId = metadataManager.registerFile(study.getId(), "file.txt", Arrays.asList("s1", "s2"));
        int fileId2 = metadataManager.registerFile(study.getId(), "file2.txt", Arrays.asList("s1", "s2"));
        metadataManager.addIndexedFiles(study.getId(), Arrays.asList(fileId, fileId2));
        int sampleId = metadataManager.getSampleId(study.getId(), "s1");
        int cohortId = metadataManager.registerCohort(study.getName(), "ALL", Arrays.asList("s1", "s2"));
        metadataManager.updateCohortMetadata(study.getId(), cohortId, cm -> {
            cm.setStatsStatus(TaskMetadata.Status.READY);
        });

        Assert.assertEquals(2, metadataManager.getFileMetadata(study.getId(), fileId).getSamples().size());
        Assert.assertEquals(2, metadataManager.getFileMetadata(study.getId(), fileId2).getSamples().size());
        Assert.assertEquals(2, metadataManager.getSampleMetadata(study.getId(), sampleId).getFiles().size());
        Assert.assertTrue(metadataManager.getCohortMetadata(study.getId(), cohortId).isStatsReady());
        Assert.assertEquals(2, metadataManager.getCohortMetadata(study.getId(), cohortId).getSamples().size());

        metadataManager.removeIndexedFiles(study.getId(), Collections.singletonList(fileId2));

        Assert.assertEquals(2, metadataManager.getFileMetadata(study.getId(), fileId).getSamples().size());
        // Files are not removed from sample metadata. We need to preserve the files order.
        Assert.assertEquals(2, metadataManager.getSampleMetadata(study.getId(), sampleId).getFiles().size());
        Assert.assertTrue(metadataManager.getCohortMetadata(study.getId(), cohortId).isInvalid());
        Assert.assertEquals(2, metadataManager.getCohortMetadata(study.getId(), cohortId).getSamples().size());
    }

}