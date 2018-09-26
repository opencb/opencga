package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.ClinicalProperty;

import java.util.*;

public class VariantClassification {

    private List<String> acmg;
    private ClinicalSignificance clinicalSignificance;
    private DrugResponse drugResponse;
    private TraitAssociation traitAssociation;
    private FunctionalEffect functionalEffect;
    private Tumorigenesis tumorigenesis;
    private List<String> other;

    public enum ClinicalSignificance {
        PATHOGENIC_VARIANT,
        LIKELY_PATHOGENIC_VARIANT,
        VARIANT_OF_UNKNOWN_CLINICAL_SIGNIFICANCE,
        LINKELY_BENIGN_VARIANT,
        BENIGN_VARIANT,
        NOT_ASSESSED
    }

    public enum DrugResponse {
        ALTERED_SENSITIVITY,
        REDUCED_SENSITIVITY,
        INCREASED_SENSITIVITY,
        ALTERED_RESISTANCE,
        INCREASED_RESISTANCE,
        REDUCED_RESISTANCE,
        INCREASED_RISK_OF_TOXICITY,
        REDUCED_RISK_OF_TOXICITY,
        ALTERED_TOXICITY,
        ADVERSE_DRUG_REACTION,
        INDICATION,
        CONTRAINDICATION,
        DOSING_ALTERATION,
        INCREASED_DOSE,
        REDUCED_DOSE,
        INCREASED_MONITORING,
        INCREASED_EFFICACY,
        REDUCED_EFFICACY,
        ALTERED_EFFICACY
    }

    public enum TraitAssociation {
        ESTABLISHED_RISK_ALLELE,
        LIKELY_RISK_ALLELE,
        UNCERTAIN_RISK_ALLELE,
        PROTECTIVE
    }

    public enum FunctionalEffect {
        DOMINANT_NEGATIVE_VARIANT,
        GAIN_OF_FUNCTION_VARIANT,
        LETHAL_VARIANT,
        LOSS_OF_FUNCTION_VARIANT,
        LOSS_OF_HETEROZYGOSITY,
        NULL_VARIANT
    }

    public enum Tumorigenesis {
        DRIVER,
        PASSENGER,
        MODIFIER
    }

    public static Set<String> LOF = new HashSet<>(Arrays.asList("transcript_ablation", "splice_acceptor_variant", "splice_donor_variant",
            "stop_gained", "frameshift_variant", "stop_lost", "start_lost", "transcript_amplification", "inframe_insertion",
            "inframe_deletion"));

    public static Set<String> PROTEIN_LENGTH_CHANGING = new HashSet<>(Arrays.asList("stop_gained", "stop_lost", "frameshift_variant",
            "inframe_insertion", "inframe_deletion", "splice_acceptor_variant", "splice_donor_variant"));

    public static List<String> calculateAcmgClassification(Variant variant) {
        return calculateAcmgClassification(variant, null);
    }

    public static List<String> calculateAcmgClassification(Variant variant, ReportedEvent reportedEvent) {
        Set<String> acmg = new HashSet<>();

        // TODO: PM1
        //   Manual: PS3, PS4, PM3
        //   ?? PM6, PP1 (Cosegregation),

        for (ConsequenceType consequenceType: variant.getAnnotation().getConsequenceTypes()) {
            for (SequenceOntologyTerm so: consequenceType.getSequenceOntologyTerms()) {
                // PVS1
                if (LOF.contains(so.getName())) {
                    acmg.add("PVS1");
                }

                // PS1
                if ("synonymous_variant".equals(so.getName())) {
                    for (EvidenceEntry evidenceEntry : variant.getAnnotation().getTraitAssociation()) {
                        if ("clinvar".equals(evidenceEntry.getSource().getName())
                                && (evidenceEntry.getVariantClassification().getClinicalSignificance() == org.opencb.biodata.models.variant.avro.ClinicalSignificance.pathogenic
                                || evidenceEntry.getVariantClassification().getClinicalSignificance() == org.opencb.biodata.models.variant.avro.ClinicalSignificance.likely_pathogenic)) {
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

                // PM5 or PP2
//                if ("missense_variant".equals(so.getName())) {
//                    acmg.add("PM5");
//                }
            }
            //  PP3, BP4
            if (consequenceType.getProteinVariantAnnotation() != null
                    && ListUtils.isNotEmpty(consequenceType.getProteinVariantAnnotation().getSubstitutionScores())
                    && ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())
                    && ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                double sift = Double.MIN_VALUE;
                double polyphen = Double.MIN_VALUE;
                double scaledCadd = Double.MIN_VALUE;
                double gerp = Double.MIN_VALUE;
                for (Score score: consequenceType.getProteinVariantAnnotation().getSubstitutionScores()) {
                    switch (score.getSource()) {
                        case "sift":
                            sift = score.getScore();
                            break;
                        case "polyphen":
                            polyphen = score.getScore();
                            break;
                    }
                }
                for (Score score: variant.getAnnotation().getFunctionalScore()) {
                    if ("cadd_scaled".equals(score.getSource())) {
                        scaledCadd = score.getScore();
                        break;
                    }
                }
                for (Score score: variant.getAnnotation().getConservation()) {
                    if ("gerp".equals(score.getSource())) {
                        gerp = score.getScore();
                        break;
                    }
                }

                if (sift != Double.MIN_VALUE && polyphen != Double.MIN_VALUE && scaledCadd != Double.MIN_VALUE
                        && gerp != Double.MIN_VALUE) {
                    if (sift < 0.05 && polyphen > 0.91 && scaledCadd > 15 && gerp > 2) {
                        acmg.add("PP3");
                    } else {
                        acmg.add("BP4");
                    }
                }
            }
        }

        if (reportedEvent != null) {
            if (reportedEvent.getModeOfInheritance() == ClinicalProperty.ModeOfInheritance.DE_NOVO) {
                acmg.add("PS2");
            } else if (reportedEvent.getModeOfInheritance() == ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS) {
                acmg.add("PM3");
            }
        }

        // PM2, BA1
        if (ListUtils.isEmpty(variant.getAnnotation().getPopulationFrequencies())) {
            acmg.add("PM2");
        } else {
            boolean above5 = false;
            boolean hasPopFreq = false;
            for (PopulationFrequency populationFrequency: variant.getAnnotation().getPopulationFrequencies()) {
                // TODO: check it!
                if (populationFrequency.getAltAlleleFreq() != 0) {
                    hasPopFreq = true;
                }
                if ("EXAC".equals(populationFrequency.getStudy())
                        || "1kG_phase3".equals(populationFrequency.getStudy())
                        || "GNOMAD_EXOMES".equals(populationFrequency.getStudy())) {
                    if (populationFrequency.getAltAlleleFreq() > 0.05) {
                        above5 = true;
                    }
                }
                if (hasPopFreq && above5) {
                    break;
                }
            }
            if (!hasPopFreq) {
                acmg.add("PM2");
            }
            if (above5) {
                acmg.add("BA1");
            }
        }

        for (EvidenceEntry evidenceEntry : variant.getAnnotation().getTraitAssociation()) {
            if ("clinvar".equals(evidenceEntry.getSource().getName())
                    && (evidenceEntry.getVariantClassification().getClinicalSignificance() == org.opencb.biodata.models.variant.avro.ClinicalSignificance.benign
                    || evidenceEntry.getVariantClassification().getClinicalSignificance() == org.opencb.biodata.models.variant.avro.ClinicalSignificance.likely_benign)) {
                acmg.add("BP6");
            } else if ("clinvar".equals(evidenceEntry.getSource().getName())
                    && (evidenceEntry.getVariantClassification().getClinicalSignificance() == org.opencb.biodata.models.variant.avro.ClinicalSignificance.pathogenic
                    || evidenceEntry.getVariantClassification().getClinicalSignificance() == org.opencb.biodata.models.variant.avro.ClinicalSignificance.likely_pathogenic)) {
                acmg.add("PP5");
            }

        }

        return new ArrayList<>(acmg);
    }

    public VariantClassification() {
        this.acmg = new ArrayList<>();
    }

    public VariantClassification(List<String> acmg, ClinicalSignificance clinicalSignificance, DrugResponse drugResponse,
                                 TraitAssociation traitAssociation, FunctionalEffect functionalEffect, Tumorigenesis tumorigenesis,
                                 List<String> other) {
        this.acmg = acmg;
        this.clinicalSignificance = clinicalSignificance;
        this.drugResponse = drugResponse;
        this.traitAssociation = traitAssociation;
        this.functionalEffect = functionalEffect;
        this.tumorigenesis = tumorigenesis;
        this.other = other;
    }

    public List<String> getAcmg() {
        return acmg;
    }

    public VariantClassification setAcmg(List<String> acmg) {
        this.acmg = acmg;
        return this;
    }

    public ClinicalSignificance getClinicalSignificance() {
        return clinicalSignificance;
    }

    public VariantClassification setClinicalSignificance(ClinicalSignificance clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
        return this;
    }

    public DrugResponse getDrugResponse() {
        return drugResponse;
    }

    public VariantClassification setDrugResponse(DrugResponse drugResponse) {
        this.drugResponse = drugResponse;
        return this;
    }

    public TraitAssociation getTraitAssociation() {
        return traitAssociation;
    }

    public VariantClassification setTraitAssociation(TraitAssociation traitAssociation) {
        this.traitAssociation = traitAssociation;
        return this;
    }

    public FunctionalEffect getFunctionalEffect() {
        return functionalEffect;
    }

    public VariantClassification setFunctionalEffect(FunctionalEffect functionalEffect) {
        this.functionalEffect = functionalEffect;
        return this;
    }

    public Tumorigenesis getTumorigenesis() {
        return tumorigenesis;
    }

    public VariantClassification setTumorigenesis(Tumorigenesis tumorigenesis) {
        this.tumorigenesis = tumorigenesis;
        return this;
    }

    public List<String> getOther() {
        return other;
    }

    public VariantClassification setOther(List<String> other) {
        this.other = other;
        return this;
    }
}
