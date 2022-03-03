package org.opencb.opencga.app.migrations.v2_2_0.storage;

import org.opencb.commons.ProgressLogger;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.Iterator;

@Migration(id = "update_sample_index_status_1901",
        description = " Improve sample-index multi-schema management. #1901", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        date = 20220302)
public class UpdateSampleIndexStatus extends StorageMigrationTool {

    // Deprecated to avoid confusion with actual "SAMPLE_INDEX_STATUS"
    protected static final String SAMPLE_INDEX_ANNOTATION_STATUS_OLD = "sampleIndex";

    protected static final String SAMPLE_INDEX_STATUS = "sampleIndexGenotypes";
    protected static final String SAMPLE_INDEX_VERSION = "sampleIndexGenotypesVersion";
    protected static final String SAMPLE_INDEX_ANNOTATION_STATUS = "sampleIndexAnnotation";
    protected static final String SAMPLE_INDEX_ANNOTATION_VERSION = "sampleIndexAnnotationVersion";

    private final String markKey = "migration_" + getAnnotation().id();
    private final int markValue = getAnnotation().patch();

    @Override
    protected void run() throws Exception {

        for (String project : getVariantStorageProjects()) {
            VariantStorageEngine engine = getVariantStorageEngineByProject(project);
            if (!engine.getStorageEngineId().equals("hadoop")) {
                logger.info("Skip project '{}'. VariantStorageEngine not in hadoop.");
                continue;
            }
            VariantStorageMetadataManager metadataManager = engine.getMetadataManager();
            for (String study : metadataManager.getStudyNames()) {
                int studyId = metadataManager.getStudyId(study);
                Iterator<SampleMetadata> it = metadataManager.sampleMetadataIterator(studyId);
                ProgressLogger progressLogger = new ProgressLogger("Update samples from study " + study);
                progressLogger.setBatchSize(1000);
                int read = 0;
                int modified = 0;
                while (it.hasNext()) {
                    SampleMetadata sampleMetadata = it.next();
                    if (needsMigration(sampleMetadata)) {
                        metadataManager.updateSampleMetadata(studyId, sampleMetadata.getId(),
                                this::updateSampleMetadata);
                        modified++;
                    }
                    read++;
                    progressLogger.increment(1);
                }
                logger.info("Finish update samples from study {}. {}/{} samples updated", study, modified, read);
            }
        }
    }

    protected void updateSampleMetadata(SampleMetadata sampleMetadata) {
        if (needsMigration(sampleMetadata)) {
            addVersionToSampleIndexStatus(sampleMetadata);
            renameOldSampleIndexAnnotationStatus(sampleMetadata);
            addVersionToSampleIndexAnnotationStatus(sampleMetadata);
//        removeOldSampleIndexStatus(sampleMetadata);
            addUpdatedMark(sampleMetadata);
        }
    }

    protected boolean needsMigration(SampleMetadata sampleMetadata) {
        return sampleMetadata.isIndexed() && sampleMetadata.getAttributes().getInt(markKey, -1) != markValue;
    }

    protected void addVersionToSampleIndexStatus(SampleMetadata sampleMetadata) {
        // This is a new status. In case of missing value (null), assume it's READY
        TaskMetadata.Status status = sampleMetadata.getStatus(SAMPLE_INDEX_STATUS, TaskMetadata.Status.READY);
        if (status != null) {
            int version = sampleMetadata.getAttributes().getInt(SAMPLE_INDEX_VERSION, -1);
            if (version == -1) {
                version = StudyMetadata.DEFAULT_SAMPLE_INDEX_VERSION;
            }
            sampleMetadata.setSampleIndexStatus(status, version);
        }
    }

    protected void renameOldSampleIndexAnnotationStatus(SampleMetadata sampleMetadata) {
        TaskMetadata.Status annotationStatus = sampleMetadata.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS, null);
        if (annotationStatus == null) {
            TaskMetadata.Status annotationOldStatus = sampleMetadata.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS_OLD, null);
            if (annotationOldStatus != null) {
                // Use old as new status
                sampleMetadata.setStatus(SAMPLE_INDEX_ANNOTATION_STATUS, annotationOldStatus);
            }
        }
    }

    protected void addVersionToSampleIndexAnnotationStatus(SampleMetadata sampleMetadata) {
        TaskMetadata.Status annotationStatus = sampleMetadata.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS, null);
        if (annotationStatus != null) {
            int version = sampleMetadata.getAttributes().getInt(SAMPLE_INDEX_ANNOTATION_VERSION, -1);
            if (version == -1) {
                version = StudyMetadata.DEFAULT_SAMPLE_INDEX_VERSION;
            }
            sampleMetadata.setSampleIndexAnnotationStatus(annotationStatus, version);
        }
    }

    private void removeOldSampleIndexStatus(SampleMetadata sampleMetadata) {
        sampleMetadata.getStatus().remove(SAMPLE_INDEX_STATUS);
        sampleMetadata.getStatus().remove(SAMPLE_INDEX_ANNOTATION_STATUS_OLD);
        sampleMetadata.getStatus().remove(SAMPLE_INDEX_ANNOTATION_STATUS);
    }


    private void addUpdatedMark(SampleMetadata sampleMetadata) {
        sampleMetadata.getAttributes().put(markKey, markValue);
    }

}
