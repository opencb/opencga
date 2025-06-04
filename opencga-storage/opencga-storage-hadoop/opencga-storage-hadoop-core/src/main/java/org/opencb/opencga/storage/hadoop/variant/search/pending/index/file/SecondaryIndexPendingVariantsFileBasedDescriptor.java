package org.opencb.opencga.storage.hadoop.variant.search.pending.index.file;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsFileBasedDescriptor;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.SEARCH_INDEX_LAST_TIMESTAMP;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema.VariantColumn.*;

public class SecondaryIndexPendingVariantsFileBasedDescriptor implements PendingVariantsFileBasedDescriptor {

    @Override
    public String name() {
        return "secondary_index";
    }

    @Override
    public Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager) {
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, TYPE.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, ALLELES.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, FULL_ANNOTATION.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, ANNOTATION_ID.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_NOT_SYNC.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_STATS_NOT_SYNC.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_UNKNOWN.bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, INDEX_STUDIES.bytes());
        for (Integer studyId : metadataManager.getStudyIds()) {
            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStudyColumn(studyId).bytes());
            for (CohortMetadata cohort : metadataManager.getCalculatedOrPartialCohorts(studyId)) {
                scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStatsColumn(studyId, cohort.getId()).bytes());
            }
        }
        return scan;
    }

    public Function<Result, Variant> getPendingEvaluatorMapper(VariantStorageMetadataManager metadataManager, boolean overwrite) {
        PendingVariantConverter converter = new PendingVariantConverter(metadataManager);
        if (overwrite) {
            // When overwriting mark all variants as pending
            return value -> converter.convert(value, VariantStorageEngine.SyncStatus.NOT_SYNCHRONIZED);
        } else {
            long ts = metadataManager.getProjectMetadata().getAttributes().getLong(SEARCH_INDEX_LAST_TIMESTAMP.key());
            return (value) -> converter.checkAndConvert(value, ts);
        }
    }

    @Override
    public int getFileBatchSize() {
        return 1_000_000;
    }

    public static class PendingVariantConverter {
        private final HBaseToVariantAnnotationConverter annotationConverter = new HBaseToVariantAnnotationConverter();
        private final HBaseToVariantStatsConverter statsConverter = new HBaseToVariantStatsConverter();
        private final VariantStorageMetadataManager metadataManager;

        public PendingVariantConverter(VariantStorageMetadataManager metadataManager) {
            this.metadataManager = metadataManager;
        }

        private Variant checkAndConvert(Result value, long ts) {
            VariantStorageEngine.SyncStatus syncStatus = HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(ts, value);
            if (syncStatus == VariantStorageEngine.SyncStatus.SYNCHRONIZED) {
                // Variant is already synchronized. Nothing to do!
                return null;
            } else {
                return convert(value, syncStatus);
            }
        }

        private Variant convert(Result value, VariantStorageEngine.SyncStatus syncStatus) {
            VariantRow variantRow = new VariantRow(value);
            Map<Integer, Map<Integer, org.opencb.biodata.models.variant.stats.VariantStats>> stats = statsConverter.convert(value);
            Variant variant = variantRow.walker().onStudy(studyId -> {
                stats.computeIfAbsent(studyId, k-> Collections.emptyMap());
            }).walk();
            for (Map.Entry<Integer, Map<Integer, VariantStats>> entry : stats.entrySet()) {
                int studyId = entry.getKey();
                StudyEntry studyEntry = new StudyEntry(metadataManager.getStudyName(studyId));
                variant.addStudyEntry(studyEntry);
                Map<Integer, VariantStats> cohortStats = entry.getValue();
                for (Map.Entry<Integer, VariantStats> cohortEntry : cohortStats.entrySet()) {
                    int cohortId = cohortEntry.getKey();
                    VariantStats variantStats = cohortEntry.getValue();
                    variantStats.setCohortId(metadataManager.getCohortName(studyId, cohortId));
                    studyEntry.addStats(variantStats);
                }
            }

            VariantAnnotation annotation;
            switch (syncStatus) {
                case NOT_SYNCHRONIZED:
                    annotation = annotationConverter.convert(value);
                    break;
                case STATS_NOT_SYNC:
                    annotation = null;
                    break;
                case STATS_NOT_SYNC_AND_STUDIES_UNKNOWN:
                case STUDIES_UNKNOWN_SYNC:
                case SYNCHRONIZED:
                default:
                    throw new IllegalStateException("Unexpected sync status: " + syncStatus);
            }
            if (annotation == null) {
                annotation = new VariantAnnotation();
            }

            if (annotation.getAdditionalAttributes() == null) {
                annotation.setAdditionalAttributes(new HashMap<>());
            }
            AdditionalAttribute additionalAttribute = annotation.getAdditionalAttributes()
                    .computeIfAbsent(GROUP_NAME.key(), k -> new AdditionalAttribute(new HashMap<>()));
            additionalAttribute.getAttribute().put(VariantField.AdditionalAttributes.INDEX_SYNCHRONIZATION.key(), syncStatus.key());

            variant.setAnnotation(annotation);

            return variant;
        }
    }
}
