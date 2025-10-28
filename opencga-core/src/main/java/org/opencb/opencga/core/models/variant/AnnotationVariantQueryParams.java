package org.opencb.opencga.core.models.variant;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.tools.ToolParams;

import static org.opencb.opencga.core.models.variant.VariantQueryParams.*;

public class AnnotationVariantQueryParams extends ToolParams {
    @DataField(description = ID_DESCR)
    private String id;
    @DataField(description = REGION_DESCR)
    private String region;
    @DataField(description = GENE_DESCR)
    private String gene;
    @DataField(description = TYPE_DESCR)
    private String type;
    @DataField(description = PANEL_DESC)
    private String panel;
    @DataField(description = PANEL_MOI_DESC)
    private String panelModeOfInheritance;
    @DataField(description = PANEL_CONFIDENCE_DESC)
    private String panelConfidence;
    @DataField(description = PANEL_ROLE_IN_CANCER_DESC)
    private String panelRoleInCancer;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @DataField(description = PANEL_INTERSECTION_DESC)
    private boolean panelIntersection;
    @DataField(description = PANEL_FEATURE_TYPE_DESC)
    private String panelFeatureType;
    @DataField(description = STATS_REF_DESCR)
    private String cohortStatsRef;
    @DataField(description = STATS_ALT_DESCR)
    private String cohortStatsAlt;
    @DataField(description = STATS_MAF_DESCR)
    private String cohortStatsMaf;
    @DataField(description = ANNOT_CONSEQUENCE_TYPE_DESCR)
    private String ct;
    @DataField(description = ANNOT_XREF_DESCR)
    private String xref;
    @DataField(description = ANNOT_BIOTYPE_DESCR)
    private String biotype;
    @DataField(description = ANNOT_PROTEIN_SUBSTITUTION_DESCR)
    private String proteinSubstitution;
    @DataField(description = ANNOT_CONSERVATION_DESCR)
    private String conservation;
    @DataField(description = ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR)
    private String populationFrequencyMaf;
    @DataField(description = ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR)
    private String populationFrequencyAlt;
    @DataField(description = ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR)
    private String populationFrequencyRef;
    @DataField(description = ANNOT_TRANSCRIPT_FLAG_DESCR)
    private String transcriptFlag;
    @DataField(description = ANNOT_FUNCTIONAL_SCORE_DESCR)
    private String functionalScore;
    @DataField(description = ANNOT_CLINICAL_DESCR)
    private String clinical;
    @DataField(description = ANNOT_CLINICAL_SIGNIFICANCE_DESCR)
    private String clinicalSignificance;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @DataField(description = ANNOT_CLINICAL_CONFIRMED_STATUS_DESCR)
    private boolean clinicalConfirmedStatus;

    public AnnotationVariantQueryParams() {
    }

    public AnnotationVariantQueryParams(Query query) {
        appendQuery(query);
    }

    public AnnotationVariantQueryParams appendQuery(Query query) {
        updateParams(query);
        return this;
    }

    public Query toQuery() {
        return new Query(toObjectMap());
    }

    public String getId() {
        return id;
    }

    public AnnotationVariantQueryParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public AnnotationVariantQueryParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getGene() {
        return gene;
    }

    public AnnotationVariantQueryParams setGene(String gene) {
        this.gene = gene;
        return this;
    }

    public String getType() {
        return type;
    }

    public AnnotationVariantQueryParams setType(String type) {
        this.type = type;
        return this;
    }

    public String getPanel() {
        return panel;
    }

    public AnnotationVariantQueryParams setPanel(String panel) {
        this.panel = panel;
        return this;
    }

    public String getPanelModeOfInheritance() {
        return panelModeOfInheritance;
    }

    public AnnotationVariantQueryParams setPanelModeOfInheritance(String panelModeOfInheritance) {
        this.panelModeOfInheritance = panelModeOfInheritance;
        return this;
    }

    public String getPanelConfidence() {
        return panelConfidence;
    }

    public AnnotationVariantQueryParams setPanelConfidence(String panelConfidence) {
        this.panelConfidence = panelConfidence;
        return this;
    }

    public String getPanelRoleInCancer() {
        return panelRoleInCancer;
    }

    public AnnotationVariantQueryParams setPanelRoleInCancer(String panelRoleInCancer) {
        this.panelRoleInCancer = panelRoleInCancer;
        return this;
    }

    public boolean getPanelIntersection() {
        return panelIntersection;
    }

    public AnnotationVariantQueryParams setPanelIntersection(boolean panelIntersection) {
        this.panelIntersection = panelIntersection;
        return this;
    }

    public String getPanelFeatureType() {
        return panelFeatureType;
    }

    public AnnotationVariantQueryParams setPanelFeatureType(String panelFeatureType) {
        this.panelFeatureType = panelFeatureType;
        return this;
    }

    public String getCohortStatsRef() {
        return cohortStatsRef;
    }

    public AnnotationVariantQueryParams setCohortStatsRef(String cohortStatsRef) {
        this.cohortStatsRef = cohortStatsRef;
        return this;
    }

    public String getCohortStatsAlt() {
        return cohortStatsAlt;
    }

    public AnnotationVariantQueryParams setCohortStatsAlt(String cohortStatsAlt) {
        this.cohortStatsAlt = cohortStatsAlt;
        return this;
    }

    public String getCohortStatsMaf() {
        return cohortStatsMaf;
    }

    public AnnotationVariantQueryParams setCohortStatsMaf(String cohortStatsMaf) {
        this.cohortStatsMaf = cohortStatsMaf;
        return this;
    }

    public String getCt() {
        return ct;
    }

    public AnnotationVariantQueryParams setCt(String ct) {
        this.ct = ct;
        return this;
    }

    public String getXref() {
        return xref;
    }

    public AnnotationVariantQueryParams setXref(String xref) {
        this.xref = xref;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public AnnotationVariantQueryParams setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getProteinSubstitution() {
        return proteinSubstitution;
    }

    public AnnotationVariantQueryParams setProteinSubstitution(String proteinSubstitution) {
        this.proteinSubstitution = proteinSubstitution;
        return this;
    }

    public String getConservation() {
        return conservation;
    }

    public AnnotationVariantQueryParams setConservation(String conservation) {
        this.conservation = conservation;
        return this;
    }

    public String getPopulationFrequencyMaf() {
        return populationFrequencyMaf;
    }

    public AnnotationVariantQueryParams setPopulationFrequencyMaf(String populationFrequencyMaf) {
        this.populationFrequencyMaf = populationFrequencyMaf;
        return this;
    }

    public String getPopulationFrequencyAlt() {
        return populationFrequencyAlt;
    }

    public AnnotationVariantQueryParams setPopulationFrequencyAlt(String populationFrequencyAlt) {
        this.populationFrequencyAlt = populationFrequencyAlt;
        return this;
    }

    public String getPopulationFrequencyRef() {
        return populationFrequencyRef;
    }

    public AnnotationVariantQueryParams setPopulationFrequencyRef(String populationFrequencyRef) {
        this.populationFrequencyRef = populationFrequencyRef;
        return this;
    }

    public String getTranscriptFlag() {
        return transcriptFlag;
    }

    public AnnotationVariantQueryParams setTranscriptFlag(String transcriptFlag) {
        this.transcriptFlag = transcriptFlag;
        return this;
    }

    public String getFunctionalScore() {
        return functionalScore;
    }

    public AnnotationVariantQueryParams setFunctionalScore(String functionalScore) {
        this.functionalScore = functionalScore;
        return this;
    }

    public String getClinicalSignificance() {
        return clinicalSignificance;
    }

    public AnnotationVariantQueryParams setClinicalSignificance(String clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
        return this;
    }

    public String getClinical() {
        return clinical;
    }

    public AnnotationVariantQueryParams setClinical(String clinical) {
        this.clinical = clinical;
        return this;
    }

    public boolean getClinicalConfirmedStatus() {
        return clinicalConfirmedStatus;
    }

    public AnnotationVariantQueryParams setClinicalConfirmedStatus(boolean clinicalConfirmedStatus) {
        this.clinicalConfirmedStatus = clinicalConfirmedStatus;
        return this;
    }
}
