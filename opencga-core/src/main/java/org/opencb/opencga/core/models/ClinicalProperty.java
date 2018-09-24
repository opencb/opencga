package org.opencb.opencga.core.models;

import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.clinical.ReportedEvent;
import org.opencb.opencga.core.models.clinical.ReportedVariant;

import java.util.*;

public class ClinicalProperty {

    public enum ModeOfInheritance {
        MONOALLELIC,
        MONOALLELIC_NOT_IMPRINTED,
        MONOALLELIC_MATERNALLY_IMPRINTED,
        MONOALLELIC_PATERNALLY_IMPRINTED,
        BIALLELIC,
        MONOALLELIC_AND_BIALLELIC,
        MONOALLELIC_AND_MORE_SEVERE_BIALLELIC,
        XLINKED_BIALLELIC,
        XLINKED_MONOALLELIC,
        YLINKED,
        MITOCHRONDRIAL,

        // Not modes of inheritance, but...
        DE_NOVO,
        COMPOUND_HETEROZYGOUS,

        UNKNOWN
    }

    public enum Penetrance {
        COMPLETE,
        INCOMPLETE
    }

    public enum RoleInCancer {
        ONCOGENE,
        TUMOR_SUPPRESSOR_GENE,
        BOTH
    }

    public static Set<String> LOF = new HashSet<>(Arrays.asList());
    public static Set<String> PROTEIN_LENGTH_CHANGING = new HashSet<>(Arrays.asList("stop_gained", "stop_lost", "frameshift_variant",
            "inframe_insertion", "inframe_deletion", "splice_acceptor_variant", "splice_donor_variant"));

    public static List<String> getAcmgClassification(ReportedVariant reportedVariant) {
        Set<String> acmg = new HashSet<>();

        // TODO: PM1
        //   Manual: PS3, PS4, PM3
        //   ?? PM6, PP1 (Cosegregation),

        for (ConsequenceType consequenceType: reportedVariant.getAnnotation().getConsequenceTypes()) {
            for (SequenceOntologyTerm so: consequenceType.getSequenceOntologyTerms()) {
                // PVS1
                if (LOF.contains(so.getName())) {
                    acmg.add("PVS1");
                }

                // PS1
                if ("synonymous_variant".equals(so.getName())) {
                    for (EvidenceEntry evidenceEntry : reportedVariant.getAnnotation().getTraitAssociation()) {
                        if ("clinvar".equals(evidenceEntry.getSource().getName())
                                && (evidenceEntry.getVariantClassification().getClinicalSignificance() == ClinicalSignificance.pathogenic
                                || evidenceEntry.getVariantClassification().getClinicalSignificance() == ClinicalSignificance.likely_pathogenic)) {
                            acmg.add("PS1");
                        } else {
                            acmg.add("BP7");
                        }
                    }
                }

                // PM4
                if (PROTEIN_LENGTH_CHANGING.contains(so.getName()) && "protein_coding".equals(consequenceType.getBiotype())) {
                    acmg.add("PM4");
                }

                // PM5 | PP2
//                if ("missense_variant".equals(so.getName())) {
//                    acmg.add("PM5");
//                }
            }
        }

        for (ReportedEvent reportedEvent : reportedVariant.getReportedEvents()) {
            if (reportedEvent.getModeOfInheritance() == ModeOfInheritance.DE_NOVO) {
                acmg.add("PS2");
            }
        }

        if (ListUtils.isEmpty(reportedVariant.getAnnotation().getPopulationFrequencies())) {
            acmg.add("PM2");
        } else {
            boolean hasPopFreq = false;
            for (PopulationFrequency populationFrequency: reportedVariant.getAnnotation().getPopulationFrequencies()) {
                // TODO: check it!
                if (populationFrequency.getAltAlleleFreq() != 0) {
                    hasPopFreq = true;
                    break;
                }
            }
            if (!hasPopFreq) {
                acmg.add("PM2");
            }
        }

        for (EvidenceEntry evidenceEntry : reportedVariant.getAnnotation().getTraitAssociation()) {
            if ("clinvar".equals(evidenceEntry.getSource().getName())
                    && (evidenceEntry.getVariantClassification().getClinicalSignificance() == ClinicalSignificance.benign
                    || evidenceEntry.getVariantClassification().getClinicalSignificance() == ClinicalSignificance.likely_benign)) {
                acmg.add("BP6");
            } else if ("clinvar".equals(evidenceEntry.getSource().getName())
                    && (evidenceEntry.getVariantClassification().getClinicalSignificance() == ClinicalSignificance.pathogenic
                    || evidenceEntry.getVariantClassification().getClinicalSignificance() == ClinicalSignificance.likely_pathogenic)) {
                acmg.add("PP5");
            }

        }

        return new ArrayList<>(acmg);
    }

}
