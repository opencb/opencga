package org.opencb.opencga.core.models.clinical;

import java.util.ArrayList;
import java.util.List;

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
