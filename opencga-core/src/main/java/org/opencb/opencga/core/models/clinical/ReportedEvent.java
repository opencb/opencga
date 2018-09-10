package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.commons.Phenotype;

import java.util.List;
import java.util.Map;

public class ReportedEvent {

    private String id;
    private List<Phenotype> phenotypes;
    private List<String> consequenceTypeIds;
    private GenomicFeature genomicFeature;
    private ReportedModeOfInheritance modeOfInheritance;
    private String panelId;
    private VariantClassification variantClassification;
    private Penetrance penetrance;
    private double score;
    @Deprecated
    private Map<String, Float> vendorSpecificScores;
    private boolean fullyExplainsPhenotype;
    private int groupOfVariants;
    private RoleInCancer roleInCancer;
    private boolean actionable;
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

    public enum Penetrance {
        COMPLETE,
        INCOMPLETE
    }

    public enum RoleInCancer {
        ONCOGENE,
        TUMOR_SUPPRESSOR_GENE,
        BOTH
    }

    public ReportedEvent() {
    }

    public ReportedEvent(String id, List<Phenotype> phenotypes, List<String> consequenceTypeIds, GenomicFeature genomicFeature,
                         ReportedModeOfInheritance modeOfInheritance, String panelId, VariantClassification variantClassification,
                         Penetrance penetrance, double score, Map<String, Float> vendorSpecificScores, boolean fullyExplainsPhenotype,
                         int groupOfVariants, RoleInCancer roleInCancer, boolean actionable, String justification, String tier) {
        this.id = id;
        this.phenotypes = phenotypes;
        this.consequenceTypeIds = consequenceTypeIds;
        this.genomicFeature = genomicFeature;
        this.modeOfInheritance = modeOfInheritance;
        this.panelId = panelId;
        this.variantClassification = variantClassification;
        this.penetrance = penetrance;
        this.score = score;
        this.vendorSpecificScores = vendorSpecificScores;
        this.fullyExplainsPhenotype = fullyExplainsPhenotype;
        this.groupOfVariants = groupOfVariants;
        this.roleInCancer = roleInCancer;
        this.actionable = actionable;
        this.justification = justification;
        this.tier = tier;
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

    public ReportedModeOfInheritance getModeOfInheritance() {
        return modeOfInheritance;
    }

    public ReportedEvent setModeOfInheritance(ReportedModeOfInheritance modeOfInheritance) {
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

    public Map<String, Float> getVendorSpecificScores() {
        return vendorSpecificScores;
    }

    public ReportedEvent setVendorSpecificScores(Map<String, Float> vendorSpecificScores) {
        this.vendorSpecificScores = vendorSpecificScores;
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
