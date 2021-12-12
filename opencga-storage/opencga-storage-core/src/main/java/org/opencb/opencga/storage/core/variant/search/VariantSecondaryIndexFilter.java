package org.opencb.opencga.storage.core.variant.search;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;

public class VariantSecondaryIndexFilter implements UnaryOperator<List<Variant>>, Predicate<Variant> {

    protected final Map<String, Integer> studiesMap;

    public VariantSecondaryIndexFilter(Map<String, Integer> studiesMap) {
        this.studiesMap = studiesMap;
    }

    @Override
    public List<Variant> apply(List<Variant> variants) {
        variants.removeIf(variant -> !test(variant));
        return variants;
    }

    /**
     * Return true if the variant needs to be updated in the secondary index.
     *
     * @param variant Variant
     * @return true/false
     */
    @Override
    public boolean test(Variant variant) {
        if (variant.getAnnotation() != null
                && variant.getAnnotation().getAdditionalAttributes() != null
                && variant.getAnnotation().getAdditionalAttributes().get(GROUP_NAME.key()) != null) {
            AdditionalAttribute additionalAttribute = variant.getAnnotation().getAdditionalAttributes().get(GROUP_NAME.key());
            String sync = additionalAttribute.getAttribute().get(VariantField.AdditionalAttributes.INDEX_SYNCHRONIZATION.key());
            if (VariantStorageEngine.SyncStatus.SYNCHRONIZED.key().equals(sync)) {
                // Discard variant!
                return false;
            } else if (VariantStorageEngine.SyncStatus.UNKNOWN.key().equals(sync)) {
                String indexedStudiesStr = additionalAttribute.getAttribute().get(VariantField.AdditionalAttributes.INDEX_STUDIES.key());
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
                            // Discard variant!
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }
}
