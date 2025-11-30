package org.opencb.opencga.storage.core.variant.search;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;

public class VariantSecondaryIndexFilter implements Task<Variant, Variant> {

    protected final Map<String, Integer> studiesMap;
    private final Map<String, Integer> cohortsIds = new HashMap<>();
    private final Map<Integer, Integer> cohortsSize = new HashMap<>();
    private final boolean functionalStatsEnabled;

    public VariantSecondaryIndexFilter(VariantStorageMetadataManager metadataManager, SearchIndexMetadata indexMetadata) {
        this.studiesMap = metadataManager.getStudies();
        for (Map.Entry<String, Integer> entry : metadataManager.getStudies().entrySet()) {
            for (CohortMetadata cohort : metadataManager.getCalculatedOrPartialCohorts(entry.getValue())) {
                cohortsIds.put(cohort.getName(), cohort.getId());
                cohortsSize.put(cohort.getId(), cohort.getSamples().size());
            }
        }
        this.functionalStatsEnabled = VariantSearchManager.isStatsFunctionalQueryEnabled(indexMetadata);
    }

    @Override
    public List<Variant> apply(List<Variant> variants) {
        variants.removeIf(variant -> getResolvedStatus(variant) == VariantSearchSyncInfo.Status.SYNCHRONIZED);
        return variants;
    }

    /**
     * Return true if the variant needs to be updated in the secondary index.
     *
     * @param variant Variant
     * @return true/false
     */
    public VariantSearchSyncInfo.Status getResolvedStatus(Variant variant) {
        return getResolvedStatus(variant, readSearchSyncInfoFromAnnotation(variant.getAnnotation())).getStatus();
    }

    /**
     * Return true if the variant needs to be updated in the secondary index.
     *
     * @param variant Variant
     * @param variantSearchSyncInfo read from the variant annotation
     * @return true/false
     */
    public VariantSearchSyncInfo getResolvedStatus(Variant variant, VariantSearchSyncInfo variantSearchSyncInfo) {
        VariantSearchSyncInfo.Status status = variantSearchSyncInfo.getStatus();

        Set<Integer> studies = getStudies(variant);
        Map<Integer, Long> currentStatsHash = getCurrentStatsHash(variant);
        if (status.isUnknown()) {
            if (status.studiesUnknown()) {
                Set<Integer> syncStudies = variantSearchSyncInfo.getStudies();
                if (syncStudies == null) {
                    // Not extra information
                    return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED, studies, currentStatsHash);
                }
                if (!studies.equals(syncStudies)) {
                    // Some studies are not indexed
                    return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED, studies, currentStatsHash);
                } else if (status == VariantSearchSyncInfo.Status.STUDIES_UNKNOWN) {
                    return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.SYNCHRONIZED, studies, currentStatsHash);
                } else if (status == VariantSearchSyncInfo.Status.STATS_AND_STUDIES_UNKNOWN) {
                    status = VariantSearchSyncInfo.Status.STATS_UNKNOWN;
                }
            }

            // Check for stats
            Map<Integer, Long> statsHash = variantSearchSyncInfo.getStatsHash();
            if (statsHash == null || statsHash.isEmpty()) {
                // No stats hash, so stats are not synchronized
                return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.STATS_NOT_SYNC, studies, currentStatsHash);
            }

            if (currentStatsHash.equals(statsHash)) {
                // Stats are synchronized
                return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.SYNCHRONIZED, studies, currentStatsHash);
            } else {
                // Stats are not synchronized
                return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.STATS_NOT_SYNC, studies, currentStatsHash);
            }
        } else {
            return new VariantSearchSyncInfo(status, studies, currentStatsHash);
        }
    }

    private Set<Integer> getStudies(Variant variant) {
        return variant.getStudies()
                .stream()
                .map(StudyEntry::getStudyId).map(studiesMap::get)
                .collect(Collectors.toSet());
    }

    private Map<Integer, Long> getCurrentStatsHash(Variant variant) {
        if (!functionalStatsEnabled) {
            // Stats hash only available if functional stats are enabled
            return Collections.emptyMap();
        }
        Map<Integer, Long> currentStatsHash = new HashMap<>();
        for (StudyEntry study : variant.getStudies()) {
            Integer studyId = studiesMap.get(study.getStudyId());
            for (VariantStats stat : study.getStats()) {
                Integer cohortId = cohortsIds.get(stat.getCohortId());
                Integer cohortSize = cohortsSize.getOrDefault(cohortId, 0);
                currentStatsHash.put(getStatsHashKey(studyId, cohortId), getStatsHashValue(stat, cohortSize));
            }
        }
        return currentStatsHash;
    }

    public static int getStatsHashKey(int studyId, int cohortId) {
//        return (cohortId + "_" + studyId).hashCode();
        // Using a simple formula to avoid collisions and keep readability
        // This will produce collisions after 10000 studies in a project, but that is not expected to happen
        return studyId + cohortId * 10000;
    }

    public static long getStatsHashValue(VariantStats stats, int cohortSize) {
        if (stats == null) {
            return 0;
        }
        int altAlleleCount = stats.getAltAlleleCount();
        int missingAllelesOrGaps = VariantSearchToVariantConverter.getAlleleMissGapCount(stats, cohortSize);
        int nonRefAlleles = VariantSearchToVariantConverter.getNonRefCount(stats);
        float passFreq = VariantSearchToVariantConverter.getPassFreq(stats);

        long b = 100000;
        // Using a base of 100000 to avoid collisions
        return altAlleleCount
                + missingAllelesOrGaps * b
                + nonRefAlleles * b * b
                + (int) (passFreq * 4) * b * b * b;
    }

    public static VariantSearchSyncInfo readSearchSyncInfoFromAnnotation(VariantAnnotation variantAnnotation) {
        if (variantAnnotation == null
                || variantAnnotation.getAdditionalAttributes() == null
                || variantAnnotation.getAdditionalAttributes().get(GROUP_NAME.key()) == null) {
            return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED);
        }
        AdditionalAttribute additionalAttribute = variantAnnotation.getAdditionalAttributes().get(GROUP_NAME.key());
        String syncStr = additionalAttribute.getAttribute().get(VariantField.AdditionalAttributes.INDEX_SYNCHRONIZATION.key());
        if (syncStr == null) {
            return new VariantSearchSyncInfo(VariantSearchSyncInfo.Status.NOT_SYNCHRONIZED);
        }
        VariantSearchSyncInfo.Status sync = VariantSearchSyncInfo.Status.from(syncStr);
        Set<Integer> studies = null;
        String indexedStudiesStr = additionalAttribute.getAttribute()
                .get(VariantField.AdditionalAttributes.INDEX_STUDIES.key());
        if (StringUtils.isNotEmpty(indexedStudiesStr)) {
            studies = Arrays.stream(indexedStudiesStr.split(","))
                    .map(Integer::valueOf)
                    .collect(Collectors.toSet());
        }
        Map<Integer, Long> statsHash = null;
        String statsHashStr = additionalAttribute.getAttribute()
                .get(VariantField.AdditionalAttributes.INDEX_STATS.key());
        if (StringUtils.isNotEmpty(statsHashStr)) {
            statsHash = new HashMap<>();
            for (String entry : statsHashStr.split(",")) {
                String[] keyValue = entry.split("=");
                if (keyValue.length == 2) {
                    statsHash.put(Integer.valueOf(keyValue[0]), Long.valueOf(keyValue[1]));
                } else {
                    throw new IllegalArgumentException("Invalid stats hash entry: " + entry);
                }
            }
        }
        return new VariantSearchSyncInfo(sync, studies, statsHash);
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
        if (searchSyncInfo.getStatsHash() != null) {
            additionalAttribute.getAttribute().put(VariantField.AdditionalAttributes.INDEX_STATS.key(),
                    searchSyncInfo.getStatsHash().entrySet().stream()
                            .map(entry -> entry.getKey() + "=" + entry.getValue())
                            .collect(Collectors.joining(",")));
        }
    }
}
