package org.opencb.opencga.core.models.clinical;

import java.util.Map;

public class ReportedEvent {

    private String id;
    private String phenotype;
    private GenomicFeature genomicFeature;
    private ReportedModeOfInheritance modeOfInheritance;
//    private Panel panel;
    private VariantClassification variantClassification;
    private Penetrance penetrance;
    private double score;
    @Deprecated
    private Map<String, Float> vendorSpecificScores;
    private boolean fullyExplainsPhenotype;
    private int groupOfVariants;
    private String justification;
    private String tier;

    public enum ReportedModeOfInheritance {
        MONOALLELIC,
        MONOALLELIC_NOT_IMPRINTED,
        MONOALLELIC_MATERNALLY_IMPRINTED,
        MONOALLELIC_PATERNALLY_IMPRINTED,
        BIALLELIC,
        MONOALLELIC_AND_BIALLELIC,
        MONOALLELIC_AND_MORE_SEVERE_BIALLELIC,
        XLINKED_BIALLELIC,
        XLINKED_MONOALLELIC,
        MITOCHRONDRIAL,
        UNKNOWN
    }

    public enum VariantClassification {
        PATHOGENIC_VARIANT,
        LIKELY_PATHOGENIC_VARIANT,
        VARIANT_OF_UNKNOWN_CLINICAL_SIGNIFICANCE,
        LINKELY_BENIGN_VARIANT,
        BENIGN_VARIANT,
        NOT_ASSESSED
    }

    public enum Penetrance {
        COMPLETE,
        INCOMPLETE
    }

    public ReportedEvent() {
    }

    public ReportedEvent(String id, String phenotype, GenomicFeature genomicFeature, ReportedModeOfInheritance modeOfInheritance,
                         VariantClassification variantClassification, Penetrance penetrance, double score, boolean fullyExplainsPhenotype,
                         int groupOfVariants, String justification, String tier) {
        this.id = id;
        this.phenotype = phenotype;
        this.genomicFeature = genomicFeature;
        this.modeOfInheritance = modeOfInheritance;
//        this.panel = panel;
        this.variantClassification = variantClassification;
        this.penetrance = penetrance;
        this.score = score;
        this.fullyExplainsPhenotype = fullyExplainsPhenotype;
        this.groupOfVariants = groupOfVariants;
        this.justification = justification;
        this.tier = tier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ReportedEvent{");
        sb.append("id='").append(id).append('\'');
        sb.append(", phenotype='").append(phenotype).append('\'');
        sb.append(", genomicFeature=").append(genomicFeature);
        sb.append(", modeOfInheritance=").append(modeOfInheritance);
//        sb.append(", panel=").append(panel);
        sb.append(", variantClassification=").append(variantClassification);
        sb.append(", penetrance=").append(penetrance);
        sb.append(", score=").append(score);
        sb.append(", fullyExplainsPhenotype=").append(fullyExplainsPhenotype);
        sb.append(", groupOfVariants=").append(groupOfVariants);
        sb.append(", eventJustification='").append(justification).append('\'');
        sb.append(", tier='").append(tier).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ReportedEvent setId(String id) {
        this.id = id;
        return this;
    }

    public String getPhenotype() {
        return phenotype;
    }

    public ReportedEvent setPhenotype(String phenotype) {
        this.phenotype = phenotype;
        return this;
    }

    public GenomicFeature getGenomicFeature() {
        return genomicFeature;
    }

    public ReportedEvent setGenomicFeature(GenomicFeature genomicFeature) {
        this.genomicFeature = genomicFeature;
        return this;
    }

    public ReportedModeOfInheritance getModeOfInheritance() {
        return modeOfInheritance;
    }

    public ReportedEvent setModeOfInheritance(ReportedModeOfInheritance modeOfInheritance) {
        this.modeOfInheritance = modeOfInheritance;
        return this;
    }

//    public Panel getPanel() {
//        return panel;
//    }
//
//    public ReportedEvent setPanel(Panel panel) {
//        this.panel = panel;
//        return this;
//    }

    public VariantClassification getVariantClassification() {
        return variantClassification;
    }

    public ReportedEvent setVariantClassification(VariantClassification variantClassification) {
        this.variantClassification = variantClassification;
        return this;
    }

    public Penetrance getPenetrance() {
        return penetrance;
    }

    public ReportedEvent setPenetrance(Penetrance penetrance) {
        this.penetrance = penetrance;
        return this;
    }

    public double getScore() {
        return score;
    }

    public ReportedEvent setScore(double score) {
        this.score = score;
        return this;
    }

    public boolean isFullyExplainsPhenotype() {
        return fullyExplainsPhenotype;
    }

    public ReportedEvent setFullyExplainsPhenotype(boolean fullyExplainsPhenotype) {
        this.fullyExplainsPhenotype = fullyExplainsPhenotype;
        return this;
    }

    public int getGroupOfVariants() {
        return groupOfVariants;
    }

    public ReportedEvent setGroupOfVariants(int groupOfVariants) {
        this.groupOfVariants = groupOfVariants;
        return this;
    }

    public String getEventJustification() {
        return justification;
    }

    public ReportedEvent setEventJustification(String justification) {
        this.justification = justification;
        return this;
    }

    public String getTier() {
        return tier;
    }

    public ReportedEvent setTier(String tier) {
        this.tier = tier;
        return this;
    }
}
