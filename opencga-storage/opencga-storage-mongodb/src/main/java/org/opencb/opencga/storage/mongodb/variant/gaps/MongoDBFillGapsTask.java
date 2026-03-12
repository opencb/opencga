package org.opencb.opencga.storage.mongodb.variant.gaps;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.gaps.AbstractFillGapsTask;
import org.opencb.opencga.storage.core.variant.gaps.VariantOverlappingStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * MongoDB implementation of {@link AbstractFillGapsTask}.
 * Collects gap-filled variants as {@link FillGapsResult} objects instead of writing to HBase.
 * The caller retrieves results via {@link #getAndClearResults()} after each {@code fillGaps()} call.
 */
public class MongoDBFillGapsTask extends AbstractFillGapsTask {

    private final List<FillGapsResult> results = new ArrayList<>();

    public MongoDBFillGapsTask(VariantStorageMetadataManager metadataManager,
                               StudyMetadata studyMetadata,
                               boolean simplifiedNewMultiAllelicVariants,
                               String gapsGenotype) {
        super(metadataManager, studyMetadata, false, simplifiedNewMultiAllelicVariants, gapsGenotype);
    }

    @Override
    protected void write(Variant variant, Set<Integer> missingSamples, VariantOverlappingStatus status) {
        results.add(new FillGapsResult(status, variant, missingSamples));
    }

    /**
     * Retrieve and clear accumulated results.
     * @return list of gap-fill results since last call
     */
    public List<FillGapsResult> getAndClearResults() {
        List<FillGapsResult> copy = new ArrayList<>(results);
        results.clear();
        return copy;
    }

    /**
     * Holds the result of a single gap-fill operation for one file at one variant position.
     */
    public static class FillGapsResult {
        private final VariantOverlappingStatus status;
        private final Variant filledVariant;
        private final Set<Integer> missingSamples;

        public FillGapsResult(VariantOverlappingStatus status, Variant filledVariant, Set<Integer> missingSamples) {
            this.status = status;
            this.filledVariant = filledVariant;
            this.missingSamples = missingSamples;
        }

        public VariantOverlappingStatus getStatus() {
            return status;
        }

        public Variant getFilledVariant() {
            return filledVariant;
        }

        public Set<Integer> getMissingSamples() {
            return missingSamples;
        }
    }
}
