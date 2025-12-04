package org.opencb.opencga.storage.core;

import org.junit.Ignore;
import org.junit.Test;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;

import static org.junit.Assert.assertEquals;

@Ignore
public abstract class VariantStorageEngineDeleteTest  extends VariantStorageBaseTest {

    private static Logger logger = LoggerFactory.getLogger(VariantStorageEngineDeleteTest.class);

    @Test
    public void testLoadAndRemove() throws Exception {
        URI outDir = newOutputUri();

        VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();

        String study1 = "study1";
        variantStorageEngine.getOptions().put(VariantStorageOptions.STUDY.key(), study1);
        variantStorageEngine.getOptions().put(VariantStorageOptions.LOAD_HOM_REF.key(), true);

        variantStorageEngine.index(Collections.singletonList(getPlatinumFile(1)), outDir);
        URI file2Uri = getPlatinumFile(2);
        variantStorageEngine.index(Collections.singletonList(file2Uri), outDir);
        variantStorageEngine.index(Collections.singletonList(getPlatinumFile(3)), outDir);

        int study1Id = mm.getStudyId(study1);
        String file2 = UriUtils.fileName(file2Uri);

        variantStorageEngine.removeFile(study1, file2, outputUri);
        LinkedHashSet<Integer> samples = mm.getSampleIdsFromFileId(study1Id, mm.getFileId(study1Id, file2));

        for (Integer sampleId : samples) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(study1Id, sampleId);
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSecondaryAnnotationIndexStatus());
            for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexStatus(v));
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(v));
            }
        }

        // Simulate that the samples were left as annotated. Ensure that the status is cleared
        for (Integer sample : samples) {
            mm.updateSampleMetadata(study1Id, sample, s -> s.setAnnotationStatus(TaskMetadata.Status.READY));
        }

        variantStorageEngine.index(Collections.singletonList(file2Uri), outDir);

        for (Integer sampleId : samples) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(study1Id, sampleId);
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSecondaryAnnotationIndexStatus());
            for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(v));
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(v));
            }
        }

        // Simulate that the samples are annotated. Ensure that the status is cleared
        for (Integer sample : samples) {
            mm.updateSampleMetadata(study1Id, sample, s -> s.setAnnotationStatus(TaskMetadata.Status.READY));
        }
        variantStorageEngine.getOptions().put(VariantStorageOptions.FORCE.key(), true);
        variantStorageEngine.index(Collections.singletonList(file2Uri), outDir);

        for (Integer sampleId : samples) {
            SampleMetadata sampleMetadata = mm.getSampleMetadata(study1Id, sampleId);
            assertEquals(TaskMetadata.Status.READY, sampleMetadata.getIndexStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getAnnotationStatus());
            assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSecondaryAnnotationIndexStatus());
            for (Integer v : sampleMetadata.getSampleIndexVersions()) {
                assertEquals(TaskMetadata.Status.READY, sampleMetadata.getSampleIndexStatus(v));
                assertEquals(TaskMetadata.Status.NONE, sampleMetadata.getSampleIndexAnnotationStatus(v));
            }
        }

    }

}
