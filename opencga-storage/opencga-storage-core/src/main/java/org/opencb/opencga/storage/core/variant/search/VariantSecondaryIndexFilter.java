package org.opencb.opencga.storage.core.variant.search;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;

public class VariantSecondaryIndexFilter implements Task<Variant, Variant> {

    protected final Map<String, Integer> studiesMap;

    public VariantSecondaryIndexFilter(Map<String, Integer> studiesMap) {
        this.studiesMap = studiesMap;
    }

    @Override
    public List<Variant> apply(List<Variant> variants) {
        variants.removeIf(variant -> getSyncStatus(variant) == VariantSearchSyncStatus.SYNCHRONIZED);
        return variants;
    }

    /**
     * Return true if the variant needs to be updated in the secondary index.
     *
     * @param variant Variant
     * @return true/false
     */
    public VariantSearchSyncStatus getSyncStatus(Variant variant) {
        if (variant.getAnnotation() != null
                && variant.getAnnotation().getAdditionalAttributes() != null
                && variant.getAnnotation().getAdditionalAttributes().get(GROUP_NAME.key()) != null) {
            AdditionalAttribute additionalAttribute = variant.getAnnotation().getAdditionalAttributes().get(GROUP_NAME.key());
            String syncStr = additionalAttribute.getAttribute().get(VariantField.AdditionalAttributes.INDEX_SYNCHRONIZATION.key());
            if (syncStr == null) {
                return VariantSearchSyncStatus.NOT_SYNCHRONIZED; // No sync status, so we need to update the secondary index
            }
            VariantSearchSyncStatus sync = VariantSearchSyncStatus.from(syncStr);
            switch (sync) {
                case SYNCHRONIZED:
                case NOT_SYNCHRONIZED:
                case STATS_NOT_SYNC:
                    return sync;
                case STUDIES_UNKNOWN_SYNC:
                case STATS_NOT_SYNC_AND_STUDIES_UNKNOWN:
                    String indexedStudiesStr = additionalAttribute.getAttribute()
                            .get(VariantField.AdditionalAttributes.INDEX_STUDIES.key());
                    if (StringUtils.isNotEmpty(indexedStudiesStr)) {
                        String[] indexedStudies = indexedStudiesStr.split(",");
                        if (indexedStudies.length == variant.getStudies().size()) {
                            Set<Integer> studies = variant.getStudies()
                                    .stream()
                                    .map(StudyEntry::getStudyId).map(studiesMap::get)
                                    .collect(Collectors.toSet());
                            boolean allStudiesIndexed = true;
                            for (String indexedStudy : indexedStudies) {
                                allStudiesIndexed &= studies.contains(Integer.valueOf(indexedStudy));
                            }
                            if (allStudiesIndexed) {
                                if (sync == VariantSearchSyncStatus.STUDIES_UNKNOWN_SYNC) {
                                    return VariantSearchSyncStatus.SYNCHRONIZED;
                                } else {
                                    return VariantSearchSyncStatus.STATS_NOT_SYNC;
                                }
                            }
                        }
                    }
                    if (sync == VariantSearchSyncStatus.STUDIES_UNKNOWN_SYNC) {
                        return VariantSearchSyncStatus.NOT_SYNCHRONIZED; // No sync status, so we need to update the secondary index
                    } else {
                        return VariantSearchSyncStatus.STATS_NOT_SYNC; // Stats not synchronized, but studies are ok
                    }
                default:
                    throw new IllegalStateException("Unknown sync status: " + sync);
            }
        }
        return VariantSearchSyncStatus.NOT_SYNCHRONIZED; // No sync status, so we need to update the secondary index
    }
}
