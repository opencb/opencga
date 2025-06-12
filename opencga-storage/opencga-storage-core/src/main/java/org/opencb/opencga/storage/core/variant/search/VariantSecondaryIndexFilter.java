package org.opencb.opencga.storage.core.variant.search;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;

public class VariantSecondaryIndexFilter implements Task<Variant, Variant> {

    protected final Map<String, Integer> studiesMap;

    public VariantSecondaryIndexFilter(Map<String, Integer> studiesMap) {
        this.studiesMap = studiesMap;
    }

    @Override
    public List<Variant> apply(List<Variant> variants) {
        variants.removeIf(variant -> getSyncStatus(variant) == VariantSearchSyncInfo.Status.SYNCHRONIZED);
        return variants;
    }

    /**
     * Return true if the variant needs to be updated in the secondary index.
     *
     * @param variant Variant
     * @return true/false
     */
    public VariantSearchSyncInfo.Status getSyncStatus(Variant variant) {
        VariantSearchSyncInfo variantSearchSyncInfo = readSearchSyncInfoFromAnnotation(variant.getAnnotation());

        VariantSearchSyncInfo.Status status = variantSearchSyncInfo.getStatus();
        switch (status) {
            case SYNCHRONIZED:
            case NOT_SYNCHRONIZED:
            case STATS_NOT_SYNC:
                return status;
            case STUDIES_UNKNOWN_SYNC:
            case STATS_NOT_SYNC_AND_STUDIES_UNKNOWN:
                List<Integer> syncStudies = variantSearchSyncInfo.getStudies();

                if (syncStudies != null) {
                    if (syncStudies.size() == variant.getStudies().size()) {
                        Set<Integer> studies = variant.getStudies()
                                .stream()
                                .map(StudyEntry::getStudyId).map(studiesMap::get)
                                .collect(Collectors.toSet());
                        boolean allStudiesIndexed = studies.containsAll(syncStudies);
                        if (allStudiesIndexed) {
                            if (status == VariantSearchSyncInfo.Status.STUDIES_UNKNOWN_SYNC) {
                                return VariantSearchSyncInfo.Status.SYNCHRONIZED;
                            } else {
                                return VariantSearchSyncInfo.Status.STATS_NOT_SYNC;
                            }
                        }
                    }
                }
                if (status == VariantSearchSyncInfo.Status.STUDIES_UNKNOWN_SYNC) {
                    return VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED; // No sync status, so we need to update the secondary index
                } else {
                    return VariantSearchSyncInfo.Status.STATS_NOT_SYNC; // Stats not synchronized, but studies are ok
                }
            default:
                throw new IllegalStateException("Unknown sync status: " + status);
        }
    }

    public static VariantSearchSyncInfo readSearchSyncInfoFromAnnotation(VariantAnnotation variantAnnotation) {
        if (variantAnnotation == null
                || variantAnnotation.getAdditionalAttributes() == null
                || variantAnnotation.getAdditionalAttributes().get(GROUP_NAME.key()) == null) {
            return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED, null);
        }
        AdditionalAttribute additionalAttribute = variantAnnotation.getAdditionalAttributes().get(GROUP_NAME.key());
        String syncStr = additionalAttribute.getAttribute().get(VariantField.AdditionalAttributes.INDEX_SYNCHRONIZATION.key());
        if (syncStr == null) {
            return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED, null);
        }
        VariantSearchSyncInfo.Status sync = VariantSearchSyncInfo.Status.from(syncStr);
        List<Integer> studies = null;
        String indexedStudiesStr = additionalAttribute.getAttribute()
                .get(VariantField.AdditionalAttributes.INDEX_STUDIES.key());
        if (StringUtils.isNotEmpty(indexedStudiesStr)) {
            studies = Arrays.stream(indexedStudiesStr.split(","))
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());
        }
        return new VariantSearchSyncInfo(sync, studies);
    }

    public static void addSearchSyncInfoToAnnotation(VariantAnnotation variantAnnotation,
                                                     VariantSearchSyncInfo searchSyncInfo) {
        if (variantAnnotation.getAdditionalAttributes() == null) {
            variantAnnotation.setAdditionalAttributes(new HashMap<>());
        }
        AdditionalAttribute additionalAttribute = variantAnnotation.getAdditionalAttributes()
                .computeIfAbsent(GROUP_NAME.key(), k -> new AdditionalAttribute(new HashMap<>()));
        addSearchSyncInfoToAnnotation(additionalAttribute, searchSyncInfo);
    }

    public static void addSearchSyncInfoToAnnotation(AdditionalAttribute additionalAttribute, VariantSearchSyncInfo searchSyncInfo) {
        additionalAttribute.getAttribute().put(VariantField.AdditionalAttributes.INDEX_SYNCHRONIZATION.key(),
                searchSyncInfo.getStatus().key());
        if (searchSyncInfo.getStudies() != null) {
            additionalAttribute.getAttribute().put(VariantField.AdditionalAttributes.INDEX_STUDIES.key(),
                    searchSyncInfo.getStudies().stream().map(Object::toString).collect(Collectors.joining(",")));
        }
    }
}
