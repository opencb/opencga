package org.opencb.opencga.core.models.variant;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class AnnotationVariantQueryParams extends ToolParams {
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_REGION_DESCRIPTION)
    private String region;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_GENE_DESCRIPTION)
    private String gene;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_TYPE_DESCRIPTION)
    private String type;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_DESCRIPTION)
    private String panel;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_MODE_OF_INHERITANCE_DESCRIPTION)
    private String panelModeOfInheritance;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_CONFIDENCE_DESCRIPTION)
    private String panelConfidence;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_ROLE_IN_CANCER_DESCRIPTION)
    private String panelRoleInCancer;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_INTERSECTION_DESCRIPTION)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean panelIntersection;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_COHORT_STATS_REF_DESCRIPTION)

    private String cohortStatsRef;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_COHORT_STATS_ALT_DESCRIPTION)
    private String cohortStatsAlt;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_COHORT_STATS_MAF_DESCRIPTION)
    private String cohortStatsMaf;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_CT_DESCRIPTION)
    private String ct;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_XREF_DESCRIPTION)
    private String xref;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_TYPE_DESCRIPTION)
    private String biotype;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_PROTEIN_SUBSTITUTION_DESCRIPTION)
    private String proteinSubstitution;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_CONSERVATION_DESCRIPTION)
    private String conservation;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_POPULATION_FREQUENCY_MAF_DESCRIPTION)
    private String populationFrequencyMaf;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_POPULATION_FREQUENCY_ALT_DESCRIPTION)
    private String populationFrequencyAlt;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_POPULATION_FREQUENCY_REF_DESCRIPTION)
    private String populationFrequencyRef;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_TRANSCRIPT_FLAG_DESCRIPTION)
    private String transcriptFlag;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_FUNCTIONAL_SCORE_DESCRIPTION)
    private String functionalScore;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_CLINICAL_DESCRIPTION)
    private String clinical;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_CLINICAL_SIGNIFICANCE_DESCRIPTION)
    private String clinicalSignificance;
    @DataField(description = ParamConstants.ANNOTATION_VARIANT_QUERY_PARAMS_CLINICAL_CONFIRMED_STATUS_DESCRIPTION)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
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
