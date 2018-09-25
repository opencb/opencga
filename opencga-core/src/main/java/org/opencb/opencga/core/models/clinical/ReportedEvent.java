package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.commons.Phenotype;

import java.util.List;

import static org.opencb.opencga.core.models.ClinicalProperty.*;

public class ReportedEvent {

    private String id;
    private List<Phenotype> phenotypes;
    private List<String> consequenceTypeIds;
    private GenomicFeature genomicFeature;
    private ModeOfInheritance modeOfInheritance;

    /**
     * This must be an ID of a panel exixting in Intepretation.panels.
     */
    private String panelId;
    private VariantClassification classification;
    private Penetrance penetrance;
    private double score;
    private boolean fullyExplainPhenotypes;
    private int groupOfVariants;
    private RoleInCancer roleInCancer;
    private boolean actionable;
    private String justification;
    private String tier;

    public ReportedEvent() {
    }

    public ReportedEvent(String id, List<Phenotype> phenotypes, List<String> consequenceTypeIds, GenomicFeature genomicFeature,
                         ModeOfInheritance modeOfInheritance, String panelId, VariantClassification classification,
                         Penetrance penetrance, double score, boolean fullyExplainPhenotypes, int groupOfVariants,
                         RoleInCancer roleInCancer, boolean actionable, String justification, String tier) {
        this.id = id;
        this.phenotypes = phenotypes;
        this.consequenceTypeIds = consequenceTypeIds;
        this.genomicFeature = genomicFeature;
        this.modeOfInheritance = modeOfInheritance;
        this.panelId = panelId;
        this.classification = classification;
        this.penetrance = penetrance;
        this.score = score;
        this.fullyExplainPhenotypes = fullyExplainPhenotypes;
        this.groupOfVariants = groupOfVariants;
        this.roleInCancer = roleInCancer;
        this.actionable = actionable;
        this.justification = justification;
        this.tier = tier;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ReportedEvent{");
        sb.append("id='").append(id).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", consequenceTypeIds=").append(consequenceTypeIds);
        sb.append(", genomicFeature=").append(genomicFeature);
        sb.append(", modeOfInheritance=").append(modeOfInheritance);
        sb.append(", panelId='").append(panelId).append('\'');
        sb.append(", classification=").append(classification);
        sb.append(", penetrance=").append(penetrance);
        sb.append(", score=").append(score);
        sb.append(", fullyExplainPhenotypes=").append(fullyExplainPhenotypes);
        sb.append(", groupOfVariants=").append(groupOfVariants);
        sb.append(", roleInCancer=").append(roleInCancer);
        sb.append(", actionable=").append(actionable);
        sb.append(", justification='").append(justification).append('\'');
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

    public List<Phenotype> getPhenotypes() {
        return phenotypes;
    }

    public ReportedEvent setPhenotypes(List<Phenotype> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<String> getConsequenceTypeIds() {
        return consequenceTypeIds;
    }

    public ReportedEvent setConsequenceTypeIds(List<String> consequenceTypeIds) {
        this.consequenceTypeIds = consequenceTypeIds;
        return this;
    }

    public GenomicFeature getGenomicFeature() {
        return genomicFeature;
    }

    public ReportedEvent setGenomicFeature(GenomicFeature genomicFeature) {
        this.genomicFeature = genomicFeature;
        return this;
    }

    public ModeOfInheritance getModeOfInheritance() {
        return modeOfInheritance;
    }

    public ReportedEvent setModeOfInheritance(ModeOfInheritance modeOfInheritance) {
        this.modeOfInheritance = modeOfInheritance;
        return this;
    }

    public String getPanelId() {
        return panelId;
    }

    public ReportedEvent setPanelId(String panelId) {
        this.panelId = panelId;
        return this;
    }

    public VariantClassification getClassification() {
        return classification;
    }

    public ReportedEvent setClassification(VariantClassification classification) {
        this.classification = classification;
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

    public boolean isFullyExplainPhenotypes() {
        return fullyExplainPhenotypes;
    }

    public ReportedEvent setFullyExplainPhenotypes(boolean fullyExplainPhenotypes) {
        this.fullyExplainPhenotypes = fullyExplainPhenotypes;
        return this;
    }

    public int getGroupOfVariants() {
        return groupOfVariants;
    }

    public ReportedEvent setGroupOfVariants(int groupOfVariants) {
        this.groupOfVariants = groupOfVariants;
        return this;
    }

    public RoleInCancer getRoleInCancer() {
        return roleInCancer;
    }

    public ReportedEvent setRoleInCancer(RoleInCancer roleInCancer) {
        this.roleInCancer = roleInCancer;
        return this;
    }

    public boolean isActionable() {
        return actionable;
    }

    public ReportedEvent setActionable(boolean actionable) {
        this.actionable = actionable;
        return this;
    }

    public String getJustification() {
        return justification;
    }

    public ReportedEvent setJustification(String justification) {
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
