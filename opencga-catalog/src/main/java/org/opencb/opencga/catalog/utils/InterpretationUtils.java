package org.opencb.opencga.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.InterpretationFindingStats;
import org.opencb.opencga.core.models.clinical.InterpretationStats;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InterpretationUtils {

    public static InterpretationStats calculateStats(Interpretation interpretation) {
        InterpretationStats stats = InterpretationStats.init();
        if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
            updateInterpretationFindingStats(interpretation.getPrimaryFindings(), stats.getPrimaryFindings());
        }
        if (CollectionUtils.isNotEmpty(interpretation.getSecondaryFindings())) {
            updateInterpretationFindingStats(interpretation.getSecondaryFindings(), stats.getSecondaryFindings());
        }
        return stats;
    }

    private static void updateInterpretationFindingStats(List<ClinicalVariant> clinicalVariantList, InterpretationFindingStats stats) {
        stats.setNumVariants(clinicalVariantList.size());

        for (ClinicalVariant clinicalVariant : clinicalVariantList) {
            if (clinicalVariant.getStatus() != null) {
                int count = stats.getVariantStatusCount().get(clinicalVariant.getStatus()) + 1;
                stats.getVariantStatusCount().put(clinicalVariant.getStatus(), count);
            }
            if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
                Set<String> tierSet = new HashSet<>();
                for (ClinicalVariantEvidence evidence : clinicalVariant.getEvidences()) {
                    if (evidence.getClassification() != null) {
                        if (StringUtils.isNotEmpty(evidence.getClassification().getTier())) {
                            tierSet.add(evidence.getClassification().getTier().toLowerCase());
                        }
                    }
                }
                if (tierSet.contains("tier1")) {
                    stats.getTierCount().setTier1(stats.getTierCount().getTier1() + 1);
                } else if (tierSet.contains("tier2")) {
                    stats.getTierCount().setTier2(stats.getTierCount().getTier2() + 1);
                } else if (tierSet.contains("tier3")) {
                    stats.getTierCount().setTier3(stats.getTierCount().getTier3() + 1);
                } else {
                    stats.getTierCount().setUntiered(stats.getTierCount().getUntiered() + 1);
                }
            }
        }
    }

}
