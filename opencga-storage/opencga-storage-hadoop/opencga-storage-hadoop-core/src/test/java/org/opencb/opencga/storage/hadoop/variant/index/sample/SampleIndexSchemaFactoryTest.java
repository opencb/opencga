package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;

import java.util.Arrays;

public class SampleIndexSchemaFactoryTest {

    private SampleIndexSchemaFactory schemaFactory;
    private StudyMetadata study1;

    @Before
    public void setUp() throws Exception {
        VariantStorageMetadataManager metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory(true));
        schemaFactory = new SampleIndexSchemaFactory(metadataManager);

        study1 = metadataManager.createStudy("study1");
        int fileId1 = metadataManager.registerFile(study1.getId(), "file1.vcf", Arrays.asList("S0", "S1", "S2", "S3", "S4"));

//        metadataManager.updateSampleMetadata(study1.getId(), metadataManager.getSampleId(study1.getId(), "S0"), sm -> {
//              // Sample index never built
//        });

        metadataManager.updateSampleMetadata(study1.getId(), metadataManager.getSampleId(study1.getId(), "S1"), sm -> {
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 1);
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 2);
            sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 1);
            sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 2);
        });

        metadataManager.updateSampleMetadata(study1.getId(), metadataManager.getSampleId(study1.getId(), "S2"), sm -> {
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 1);
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 2);
            sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 1);
            // Annotation not yet built for version 2!
        });

        metadataManager.updateSampleMetadata(study1.getId(), metadataManager.getSampleId(study1.getId(), "S3"), sm -> {
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 1);
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 3);
            sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 1);
            sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 3);
            // Version 2 index never built
        });

        metadataManager.updateSampleMetadata(study1.getId(), metadataManager.getSampleId(study1.getId(), "S4"), sm -> {
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 1);
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 2);
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 3);
            sm.setSampleIndexStatus(TaskMetadata.Status.READY, 4);
            sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 1);
            sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 2);
            sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 3);
            sm.setSampleIndexAnnotationStatus(TaskMetadata.Status.READY, 4);
        });
    }

    @Test
    public void testGetVersion() {
        Assert.assertEquals(Arrays.asList(), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S0")));
        Assert.assertEquals(Arrays.asList(1, 2), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S1")));
        Assert.assertEquals(Arrays.asList(1), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S2")));
        Assert.assertEquals(Arrays.asList(1, 3), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S3")));
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S4")));

        Assert.assertEquals(Arrays.asList(1), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S1", "S2")));
        Assert.assertEquals(Arrays.asList(1), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S2", "S3")));
        Assert.assertEquals(Arrays.asList(1), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S1", "S3")));
        Assert.assertEquals(Arrays.asList(1), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S1", "S2", "S3")));
        Assert.assertEquals(Arrays.asList(), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S1", "S2", "S3", "S0")));

        Assert.assertEquals(Arrays.asList(1, 2), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S1", "S4")));
        Assert.assertEquals(Arrays.asList(1), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S2", "S4")));
        Assert.assertEquals(Arrays.asList(1, 3), schemaFactory.getSampleIndexConfigurationVersions(study1.getId(), Arrays.asList("S3", "S4")));
    }
}