package org.opencb.opencga.app.migrations.v2_2_0.storage;

import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;

import static org.junit.Assert.*;

public class UpdateSampleIndexStatusTest {

    private UpdateSampleIndexStatus migrationTool;
    private SampleMetadata sm;

    @Before
    public void setUp() throws Exception {
        migrationTool = new UpdateSampleIndexStatus();
    }

    @Test
    public void testUpdateUnindexed() {
        sm = new SampleMetadata(1, 1, "1");
        assertFalse(migrationTool.needsMigration(sm));
        migrationTool.updateSampleMetadata(sm);

        check(null, null);
    }

    @Test
    public void testUpdateIndexedWithoutSampleIndex() {
        sm = new SampleMetadata(1, 1, "1")
                .setIndexStatus(TaskMetadata.Status.READY)
                .setStatus(UpdateSampleIndexStatus.SAMPLE_INDEX_STATUS, TaskMetadata.Status.NONE);
        assertTrue(migrationTool.needsMigration(sm));
        migrationTool.updateSampleMetadata(sm);
        assertFalse(migrationTool.needsMigration(sm));

        check(null, null);
    }

    @Test
    public void testUpdateUnversioned() {
        sm = new SampleMetadata(1, 1, "1")
                .setIndexStatus(TaskMetadata.Status.READY)
                .setStatus(UpdateSampleIndexStatus.SAMPLE_INDEX_ANNOTATION_STATUS_OLD, TaskMetadata.Status.READY);
        assertTrue(migrationTool.needsMigration(sm));
        migrationTool.updateSampleMetadata(sm);
        assertFalse(migrationTool.needsMigration(sm));

        check(1, 1);
    }

    @Test
    public void testUpdateVersionedPartial() {
        sm = new SampleMetadata(1, 1, "1")
                .setIndexStatus(TaskMetadata.Status.READY)
                .setStatus(UpdateSampleIndexStatus.SAMPLE_INDEX_ANNOTATION_STATUS_OLD, TaskMetadata.Status.READY)
                .putAttribute(UpdateSampleIndexStatus.SAMPLE_INDEX_VERSION, 2);
        assertTrue(migrationTool.needsMigration(sm));
        migrationTool.updateSampleMetadata(sm);
        assertFalse(migrationTool.needsMigration(sm));

        check(2, 1);
    }

    @Test
    public void testUpdateVersioned() {
        sm = new SampleMetadata(1, 1, "1")
                .setIndexStatus(TaskMetadata.Status.READY)
                .setStatus(UpdateSampleIndexStatus.SAMPLE_INDEX_ANNOTATION_STATUS_OLD, TaskMetadata.Status.READY)
                .putAttribute(UpdateSampleIndexStatus.SAMPLE_INDEX_VERSION, 3)
                .putAttribute(UpdateSampleIndexStatus.SAMPLE_INDEX_ANNOTATION_VERSION, 2);
        assertTrue(migrationTool.needsMigration(sm));
        migrationTool.updateSampleMetadata(sm);
        assertFalse(migrationTool.needsMigration(sm));

        check(3, 2);

    }

    private void check(Integer sampleIndexVersion, Integer sampleIndexAnnotationVersion) {
        System.out.println("sm = " + sm.toString());
        assertEquals(sampleIndexVersion, sm.getSampleIndexVersion());
        assertEquals(sampleIndexAnnotationVersion, sm.getSampleIndexAnnotationVersion());

        if (sampleIndexVersion != null) {
            for (int i = 0; i < sampleIndexVersion - 1; i++) {
                assertEquals(TaskMetadata.Status.NONE, sm.getSampleIndexStatus(i));
            }
            assertEquals(TaskMetadata.Status.READY, sm.getSampleIndexStatus(sampleIndexVersion));
        }
        if (sampleIndexAnnotationVersion != null) {
            for (int i = 0; i < sampleIndexAnnotationVersion - 1; i++) {
                assertEquals(TaskMetadata.Status.NONE, sm.getSampleIndexAnnotationStatus(i));
            }
            assertEquals(TaskMetadata.Status.READY, sm.getSampleIndexAnnotationStatus(sampleIndexAnnotationVersion));
        }
    }
}