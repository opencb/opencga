package org.opencb.opencga.catalog.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
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
            // Populate variant status
            if (clinicalVariant.getStatus() != null) {
                int count = stats.getVariantStatusCount().get(clinicalVariant.getStatus()) + 1;
                stats.getVariantStatusCount().put(clinicalVariant.getStatus(), count);
            }

            // Populate tierCount
            if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
                Set<String> tierSet = new HashSet<>();
                for (ClinicalVariantEvidence evidence : clinicalVariant.getEvidences()) {
                    if (evidence.getClassification() != null) {
                        if (StringUtils.isNotEmpty(evidence.getClassification().getTier())) {
                            tierSet.add(evidence.getClassification().getTier());
                        }
                    }
                }
                for (String tierKey : tierSet) {
                    if (!stats.getTierCount().containsKey(tierKey)) {
                        stats.getTierCount().put(tierKey, 0);
                    }
                    stats.getTierCount().put(tierKey, stats.getTierCount().get(tierKey));
                }
            }

            // Populate geneCount
            if (clinicalVariant.getAnnotation() != null) {
                if (CollectionUtils.isNotEmpty(clinicalVariant.getAnnotation().getConsequenceTypes())) {
                    Set<String> geneSet = new HashSet<>();
                    for (ConsequenceType consequenceType : clinicalVariant.getAnnotation().getConsequenceTypes()) {
                        if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                            geneSet.add(consequenceType.getGeneName());
                        }
                    }
                    for (String geneKey : geneSet) {
                        if (!stats.getGeneCount().containsKey(geneKey)) {
                            stats.getGeneCount().put(geneKey, 0);
                        }
                        stats.getGeneCount().put(geneKey, stats.getGeneCount().get(geneKey));
                    }
                }
            }
        }
    }

}
