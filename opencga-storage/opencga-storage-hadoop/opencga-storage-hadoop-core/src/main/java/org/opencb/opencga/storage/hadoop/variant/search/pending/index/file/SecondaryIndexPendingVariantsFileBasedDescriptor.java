package org.opencb.opencga.storage.hadoop.variant.search.pending.index.file;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsFileBasedDescriptor;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.SEARCH_INDEX_LAST_TIMESTAMP;
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
            return converter::convert;
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
        private final HBaseToVariantAnnotationConverter converter = new HBaseToVariantAnnotationConverter();
        private final HBaseToVariantStatsConverter statsConverter = new HBaseToVariantStatsConverter();
        private final VariantStorageMetadataManager metadataManager;

        public PendingVariantConverter(VariantStorageMetadataManager metadataManager) {
            this.metadataManager = metadataManager;
        }

        private Variant checkAndConvert(Result value, long ts) {
            VariantStorageEngine.SyncStatus syncStatus = HadoopVariantSearchIndexUtils.getSyncStatusCheckStudies(ts, value);
            boolean pending = syncStatus != VariantStorageEngine.SyncStatus.SYNCHRONIZED;
            if (pending) {
                return convert(value);
            } else {
                return null;
            }
        }

        private Variant convert(Result value) {
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

            VariantAnnotation annotation = converter.convert(value);
            if (annotation != null) {
                variant.setAnnotation(annotation);
            }
            return variant;
        }
    }
}
