package org.opencb.opencga.app.migrations.v2_8_6.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.util.Map;

@Migration(id = "mark_unknown_largest_variant_length" ,
        description = "Mark as unknown largest variant length",
        version = "2.8.6",
        domain = Migration.MigrationDomain.STORAGE,
        language = Migration.MigrationLanguage.JAVA,
        patch = 1,
        date = 20230927
)
public class MarkUnknownLargestVariantLength extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
        for (String project : getVariantStorageProjects()) {
            try (VariantStorageEngine engine = getVariantStorageEngineByProject(project)) {
                for (Map.Entry<String, Integer> entry : engine.getMetadataManager().getStudies().entrySet()) {
                    String studyName = entry.getKey();
                    Integer studyId = entry.getValue();
                    logger.info("Process study '" + studyName + "' (" + studyId + ")");
                    // Check for indexed samples with undefined largest variant length
                    for (SampleMetadata sampleMetadata : engine.getMetadataManager().sampleMetadataIterable(studyId)) {
                        if (sampleMetadata.isIndexed()) {
                            if (!sampleMetadata.getAttributes().containsKey(SampleIndexSchema.LARGEST_VARIANT_LENGTH)) {
                                logger.info("Mark unknown largest variant length for sample '" + sampleMetadata.getName() + "'");
                                engine.getMetadataManager().updateSampleMetadata(studyId, sampleMetadata.getId(),
                                        sm -> sm.getAttributes().put(SampleIndexSchema.UNKNOWN_LARGEST_VARIANT_LENGTH, true));
                            }
                        }
                    }
                }
            }
        }
    }
}
