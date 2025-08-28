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
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.search.VariantSearchSyncInfo;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsFileBasedDescriptor;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;

public class SecondaryIndexPendingVariantsFileBasedDescriptor implements PendingVariantsFileBasedDescriptor {

    @Override
    public String name() {
        return "secondary_index";
    }

    @Override
    public Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager) {
        return HadoopVariantSearchIndexUtils.configureScan(scan, metadataManager);
    }

    public Function<Result, Variant> getPendingEvaluatorMapper(VariantStorageMetadataManager metadataManager, boolean overwrite) {
        SearchIndexMetadata indexMetadata = metadataManager.getProjectMetadata().getSecondaryAnnotationIndex()
                .getSearchIndexMetadataForLoading();
        PendingVariantConverter converter = new PendingVariantConverter(metadataManager, indexMetadata);
        long creationTs = indexMetadata.getCreationDateTimestamp();
        long updateTs = indexMetadata.getLastUpdateDateTimestamp();
        if (overwrite) {
            // When overwriting mark all variants as pending
            return value -> converter.convert(value, VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED, updateTs);
        } else {
            return (value) -> converter.checkAndConvert(value, creationTs, updateTs);
        }
    }

    @Override
    public int getFileBatchSize() {
        return 1_000_000;
    }

    public static class PendingVariantConverter {
        private final HBaseToVariantAnnotationConverter annotationConverter = new HBaseToVariantAnnotationConverter();
        private final HBaseToVariantStatsConverter statsConverter = new HBaseToVariantStatsConverter();
        private final Map<Integer, Integer> cohortsSize;
        private final Map<Integer, String> studyNames;
        private final VariantStorageMetadataManager metadataManager;

        public PendingVariantConverter(VariantStorageMetadataManager metadataManager, SearchIndexMetadata indexMetadata) {
            this.metadataManager = metadataManager;
            if (VariantSearchManager.isStatsFunctionalQueryEnabled(indexMetadata)) {
                this.cohortsSize = new HashMap<>();
                for (Map.Entry<String, Integer> entry : metadataManager.getStudies().entrySet()) {
                    for (CohortMetadata cohort : metadataManager.getCalculatedOrPartialCohorts(entry.getValue())) {
                        cohortsSize.put(cohort.getId(), cohort.getSamples().size());
                    }
                }
            } else {
                this.cohortsSize = null;
            }
            studyNames = metadataManager.getStudies().inverse();
        }

        private Variant checkAndConvert(Result value, long creationDateTs, long lastUpdateTs) {
            VariantSearchSyncInfo.Status syncStatus = HadoopVariantSearchIndexUtils
                    .getSyncStatusInfoResolved(creationDateTs, lastUpdateTs, value, cohortsSize);
            if (syncStatus == VariantSearchSyncInfo.Status.SYNCHRONIZED) {
                // Variant is already synchronized. Nothing to do!
                return null;
            } else {
                return convert(value, syncStatus, lastUpdateTs);
            }
        }

        private Variant convert(Result value, VariantSearchSyncInfo.Status syncStatus, long lastUpdateTs) {
            VariantRow variantRow = new VariantRow(value);
            Map<Integer, Map<Integer, org.opencb.biodata.models.variant.stats.VariantStats>> stats = new HashMap<>();
            Variant variant = variantRow.walker()
                    .onStudy(studyId -> {
                        stats.computeIfAbsent(studyId, k -> Collections.emptyMap());
                    }).onCohortStats(statsColumn -> {
                        if (syncStatus == VariantSearchSyncInfo.Status.STATS_NOT_SYNC) {
                            if (statsColumn.getTimestamp() < lastUpdateTs) {
                                // Skip stats that are older than the last update timestamp
                                return;
                            }
                        }
                        VariantStats variantStats = statsConverter.convert(statsColumn);
                        stats.computeIfAbsent(statsColumn.getStudyId(), k -> new HashMap<>())
                                .put(statsColumn.getCohortId(), variantStats);
                    }).walk();
            for (Map.Entry<Integer, Map<Integer, VariantStats>> entry : stats.entrySet()) {
                int studyId = entry.getKey();
                StudyEntry studyEntry = new StudyEntry(studyNames.get(studyId));
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
                case STATS_AND_STUDIES_UNKNOWN:
                case STUDIES_UNKNOWN:
                case STATS_UNKNOWN:
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
