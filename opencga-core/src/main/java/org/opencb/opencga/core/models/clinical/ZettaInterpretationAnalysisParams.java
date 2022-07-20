package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ZettaInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Zetta interpretation analysis params";

    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION)
    private String clinicalAnalysis;

    // Variant filters
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_ID_DESCRIPTION)
    private List<String> id;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_REGION_DESCRIPTION)
    private String region;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_TYPE_DESCRIPTION)
    private String type;

    // Study filters
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_STUDY_DESCRIPTION)
    private String study;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FILE_DESCRIPTION)
    private String file;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FILTER_DESCRIPTION)
    private String filter;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_QUAL_DESCRIPTION)
    private String qual;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FILE_DATA_DESCRIPTION)
    private String fileData;

    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION)
    private String sample;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SAMPLE_DATA_DESCRIPTION)
    private String sampleData;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SAMPLE_ANNOTATION_DESCRIPTION)
    private String sampleAnnotation;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SAMPLE_METADATA_DESCRIPTION)
    private String sampleMetadata;

    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_DESCRIPTION)
    private String cohort;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_REF_DESCRIPTION)
    private String cohortStatsRef;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_ALT_DESCRIPTION)
    private String cohortStatsAlt;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_MAF_DESCRIPTION)
    private String cohortStatsMaf;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_MGF_DESCRIPTION)
    private String cohortStatsMgf;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_PASS_DESCRIPTION)
    private String cohortStatsPass;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SCORE_DESCRIPTION)
    private String score;

    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_DESCRIPTION)
    private String family;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_DISORDER_DESCRIPTION)
    private String familyDisorder;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_SEGREGATION_DESCRIPTION)
    private String familySegregation;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_MEMBERS_DESCRIPTION)
    private String familyMembers;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_PROBAND_DESCRIPTION)
    private String familyProband;

    // Annotation filters
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_GENE_DESCRIPTION)
    private String gene;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CT_DESCRIPTION)
    private String ct;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_XREF_DESCRIPTION)
    private String xref;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_TYPE_DESCRIPTION)
    private String biotype;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PROTEIN_SUBSTITUTION_DESCRIPTION)
    private String proteinSubstitution;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CONSERVATION_DESCRIPTION)
    private String conservation;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_POPULATION_FREQUENCY_ALT_DESCRIPTION)
    private String populationFrequencyAlt;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_POPULATION_FREQUENCY_REF_DESCRIPTION)
    private String populationFrequencyRef;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_POPULATION_FREQUENCY_MAF_DESCRIPTION)
    private String populationFrequencyMaf;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_TRANSCRIPT_FLAG_DESCRIPTION)
    private String transcriptFlag;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_GENE_TRAIT_ID_DESCRIPTION)
    private String geneTraitId;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_GO_DESCRIPTION)
    private String go;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_EXPRESSION_DESCRIPTION)
    private String expression;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PROTEIN_KEYWORD_DESCRIPTION)
    private String proteinKeyword;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_DRUG_DESCRIPTION)
    private String drug;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FUNCTIONAL_SCORE_DESCRIPTION)
    private String functionalScore;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_DESCRIPTION)
    private String clinical;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_SIGNIFICANCE_DESCRIPTION)
    private String clinicalSignificance;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_CONFIRMED_STATUS_DESCRIPTION)
    private boolean clinicalConfirmedStatus;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CUSTOM_ANNOTATION_DESCRIPTION)
    private String customAnnotation;

    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PANEL_DESCRIPTION)
    private String panel;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PANEL_MODE_OF_INHERITANCE_DESCRIPTION)
    private String panelModeOfInheritance;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PANEL_CONFIDENCE_DESCRIPTION)
    private String panelConfidence;
    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PANEL_ROLE_IN_CANCER_DESCRIPTION)
    private String panelRoleInCancer;

    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_TRAIT_DESCRIPTION)
    private String trait;

    @DataField(description = ParamConstants.ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PRIMARY_DESCRIPTION)
    private boolean primary; // primary interpretation (vs secondary interpretation)

    public ZettaInterpretationAnalysisParams() {
    }

    public ZettaInterpretationAnalysisParams(String clinicalAnalysis, List<String> id, String region, String type, String study,
                                             String file, String filter, String qual, String fileData, String sample, String sampleData,
                                             String sampleAnnotation, String sampleMetadata, String cohort, String cohortStatsRef,
                                             String cohortStatsAlt, String cohortStatsMaf, String cohortStatsMgf, String cohortStatsPass,
                                             String score, String family, String familyDisorder, String familySegregation,
                                             String familyMembers, String familyProband, String gene, String ct, String xref,
                                             String biotype, String proteinSubstitution, String conservation, String populationFrequencyAlt,
                                             String populationFrequencyRef, String populationFrequencyMaf, String transcriptFlag,
                                             String geneTraitId, String go, String expression, String proteinKeyword, String drug,
                                             String functionalScore, String clinicalSignificance, String customAnnotation, String panel,
                                             String trait, boolean primary) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.id = id;
        this.region = region;
        this.type = type;
        this.study = study;
        this.file = file;
        this.filter = filter;
        this.qual = qual;
        this.fileData = fileData;
        this.sample = sample;
        this.sampleData = sampleData;
        this.sampleAnnotation = sampleAnnotation;
        this.sampleMetadata = sampleMetadata;
        this.cohort = cohort;
        this.cohortStatsRef = cohortStatsRef;
        this.cohortStatsAlt = cohortStatsAlt;
        this.cohortStatsMaf = cohortStatsMaf;
        this.cohortStatsMgf = cohortStatsMgf;
        this.cohortStatsPass = cohortStatsPass;
        this.score = score;
        this.family = family;
        this.familyDisorder = familyDisorder;
        this.familySegregation = familySegregation;
        this.familyMembers = familyMembers;
        this.familyProband = familyProband;
        this.gene = gene;
        this.ct = ct;
        this.xref = xref;
        this.biotype = biotype;
        this.proteinSubstitution = proteinSubstitution;
        this.conservation = conservation;
        this.populationFrequencyAlt = populationFrequencyAlt;
        this.populationFrequencyRef = populationFrequencyRef;
        this.populationFrequencyMaf = populationFrequencyMaf;
        this.transcriptFlag = transcriptFlag;
        this.geneTraitId = geneTraitId;
        this.go = go;
        this.expression = expression;
        this.proteinKeyword = proteinKeyword;
        this.drug = drug;
        this.functionalScore = functionalScore;
        this.clinicalSignificance = clinicalSignificance;
        this.customAnnotation = customAnnotation;
        this.panel = panel;
        this.trait = trait;
        this.primary = primary;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ZettaInterpretationAnalysisParams{");
        sb.append("clinicalAnalysis='").append(clinicalAnalysis).append('\'');
        sb.append(", id=").append(id);
        sb.append(", region='").append(region).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", study='").append(study).append('\'');
        sb.append(", file='").append(file).append('\'');
        sb.append(", filter='").append(filter).append('\'');
        sb.append(", qual='").append(qual).append('\'');
        sb.append(", fileData='").append(fileData).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", sampleData='").append(sampleData).append('\'');
        sb.append(", sampleAnnotation='").append(sampleAnnotation).append('\'');
        sb.append(", sampleMetadata='").append(sampleMetadata).append('\'');
        sb.append(", cohort='").append(cohort).append('\'');
        sb.append(", cohortStatsRef='").append(cohortStatsRef).append('\'');
        sb.append(", cohortStatsAlt='").append(cohortStatsAlt).append('\'');
        sb.append(", cohortStatsMaf='").append(cohortStatsMaf).append('\'');
        sb.append(", cohortStatsMgf='").append(cohortStatsMgf).append('\'');
        sb.append(", cohortStatsPass='").append(cohortStatsPass).append('\'');
        sb.append(", score='").append(score).append('\'');
        sb.append(", family='").append(family).append('\'');
        sb.append(", familyDisorder='").append(familyDisorder).append('\'');
        sb.append(", familySegregation='").append(familySegregation).append('\'');
        sb.append(", familyMembers='").append(familyMembers).append('\'');
        sb.append(", familyProband='").append(familyProband).append('\'');
        sb.append(", gene='").append(gene).append('\'');
        sb.append(", ct='").append(ct).append('\'');
        sb.append(", xref='").append(xref).append('\'');
        sb.append(", biotype='").append(biotype).append('\'');
        sb.append(", proteinSubstitution='").append(proteinSubstitution).append('\'');
        sb.append(", conservation='").append(conservation).append('\'');
        sb.append(", populationFrequencyAlt='").append(populationFrequencyAlt).append('\'');
        sb.append(", populationFrequencyRef='").append(populationFrequencyRef).append('\'');
        sb.append(", populationFrequencyMaf='").append(populationFrequencyMaf).append('\'');
        sb.append(", transcriptFlag='").append(transcriptFlag).append('\'');
        sb.append(", geneTraitId='").append(geneTraitId).append('\'');
        sb.append(", go='").append(go).append('\'');
        sb.append(", expression='").append(expression).append('\'');
        sb.append(", proteinKeyword='").append(proteinKeyword).append('\'');
        sb.append(", drug='").append(drug).append('\'');
        sb.append(", functionalScore='").append(functionalScore).append('\'');
        sb.append(", clinicalSignificance='").append(clinicalSignificance).append('\'');
        sb.append(", customAnnotation='").append(customAnnotation).append('\'');
        sb.append(", panel='").append(panel).append('\'');
        sb.append(", trait='").append(trait).append('\'');
        sb.append(", primary=").append(primary);
        sb.append('}');
        return sb.toString();
    }

    public String getClinicalAnalysis() {
        return clinicalAnalysis;
    }

    public ZettaInterpretationAnalysisParams setClinicalAnalysis(String clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;
        return this;
    }

    public List<String> getId() {
        return id;
    }

    public ZettaInterpretationAnalysisParams setId(List<String> id) {
        this.id = id;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public ZettaInterpretationAnalysisParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getType() {
        return type;
    }

    public ZettaInterpretationAnalysisParams setType(String type) {
        this.type = type;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public ZettaInterpretationAnalysisParams setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getFile() {
        return file;
    }

    public ZettaInterpretationAnalysisParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public ZettaInterpretationAnalysisParams setFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public String getQual() {
        return qual;
    }

    public ZettaInterpretationAnalysisParams setQual(String qual) {
        this.qual = qual;
        return this;
    }

    public String getFileData() {
        return fileData;
    }

    public ZettaInterpretationAnalysisParams setFileData(String fileData) {
        this.fileData = fileData;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public ZettaInterpretationAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getSampleData() {
        return sampleData;
    }

    public ZettaInterpretationAnalysisParams setSampleData(String sampleData) {
        this.sampleData = sampleData;
        return this;
    }

    public String getSampleAnnotation() {
        return sampleAnnotation;
    }

    public ZettaInterpretationAnalysisParams setSampleAnnotation(String sampleAnnotation) {
        this.sampleAnnotation = sampleAnnotation;
        return this;
    }

    public String getSampleMetadata() {
        return sampleMetadata;
    }

    public ZettaInterpretationAnalysisParams setSampleMetadata(String sampleMetadata) {
        this.sampleMetadata = sampleMetadata;
        return this;
    }

    public String getCohort() {
        return cohort;
    }

    public ZettaInterpretationAnalysisParams setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public String getCohortStatsRef() {
        return cohortStatsRef;
    }

    public ZettaInterpretationAnalysisParams setCohortStatsRef(String cohortStatsRef) {
        this.cohortStatsRef = cohortStatsRef;
        return this;
    }

    public String getCohortStatsAlt() {
        return cohortStatsAlt;
    }

    public ZettaInterpretationAnalysisParams setCohortStatsAlt(String cohortStatsAlt) {
        this.cohortStatsAlt = cohortStatsAlt;
        return this;
    }

    public String getCohortStatsMaf() {
        return cohortStatsMaf;
    }

    public ZettaInterpretationAnalysisParams setCohortStatsMaf(String cohortStatsMaf) {
        this.cohortStatsMaf = cohortStatsMaf;
        return this;
    }

    public String getCohortStatsMgf() {
        return cohortStatsMgf;
    }

    public ZettaInterpretationAnalysisParams setCohortStatsMgf(String cohortStatsMgf) {
        this.cohortStatsMgf = cohortStatsMgf;
        return this;
    }

    public String getCohortStatsPass() {
        return cohortStatsPass;
    }

    public ZettaInterpretationAnalysisParams setCohortStatsPass(String cohortStatsPass) {
        this.cohortStatsPass = cohortStatsPass;
        return this;
    }

    public String getScore() {
        return score;
    }

    public ZettaInterpretationAnalysisParams setScore(String score) {
        this.score = score;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public ZettaInterpretationAnalysisParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getFamilyDisorder() {
        return familyDisorder;
    }

    public ZettaInterpretationAnalysisParams setFamilyDisorder(String familyDisorder) {
        this.familyDisorder = familyDisorder;
        return this;
    }

    public String getFamilySegregation() {
        return familySegregation;
    }

    public ZettaInterpretationAnalysisParams setFamilySegregation(String familySegregation) {
        this.familySegregation = familySegregation;
        return this;
    }

    public String getFamilyMembers() {
        return familyMembers;
    }

    public ZettaInterpretationAnalysisParams setFamilyMembers(String familyMembers) {
        this.familyMembers = familyMembers;
        return this;
    }

    public String getFamilyProband() {
        return familyProband;
    }

    public ZettaInterpretationAnalysisParams setFamilyProband(String familyProband) {
        this.familyProband = familyProband;
        return this;
    }

    public String getGene() {
        return gene;
    }

    public ZettaInterpretationAnalysisParams setGene(String gene) {
        this.gene = gene;
        return this;
    }

    public String getCt() {
        return ct;
    }

    public ZettaInterpretationAnalysisParams setCt(String ct) {
        this.ct = ct;
        return this;
    }

    public String getXref() {
        return xref;
    }

    public ZettaInterpretationAnalysisParams setXref(String xref) {
        this.xref = xref;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public ZettaInterpretationAnalysisParams setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getProteinSubstitution() {
        return proteinSubstitution;
    }

    public ZettaInterpretationAnalysisParams setProteinSubstitution(String proteinSubstitution) {
        this.proteinSubstitution = proteinSubstitution;
        return this;
    }

    public String getConservation() {
        return conservation;
    }

    public ZettaInterpretationAnalysisParams setConservation(String conservation) {
        this.conservation = conservation;
        return this;
    }

    public String getPopulationFrequencyAlt() {
        return populationFrequencyAlt;
    }

    public ZettaInterpretationAnalysisParams setPopulationFrequencyAlt(String populationFrequencyAlt) {
        this.populationFrequencyAlt = populationFrequencyAlt;
        return this;
    }

    public String getPopulationFrequencyRef() {
        return populationFrequencyRef;
    }

    public ZettaInterpretationAnalysisParams setPopulationFrequencyRef(String populationFrequencyRef) {
        this.populationFrequencyRef = populationFrequencyRef;
        return this;
    }

    public String getPopulationFrequencyMaf() {
        return populationFrequencyMaf;
    }

    public ZettaInterpretationAnalysisParams setPopulationFrequencyMaf(String populationFrequencyMaf) {
        this.populationFrequencyMaf = populationFrequencyMaf;
        return this;
    }

    public String getTranscriptFlag() {
        return transcriptFlag;
    }

    public ZettaInterpretationAnalysisParams setTranscriptFlag(String transcriptFlag) {
        this.transcriptFlag = transcriptFlag;
        return this;
    }

    public String getGeneTraitId() {
        return geneTraitId;
    }

    public ZettaInterpretationAnalysisParams setGeneTraitId(String geneTraitId) {
        this.geneTraitId = geneTraitId;
        return this;
    }

    public String getGo() {
        return go;
    }

    public ZettaInterpretationAnalysisParams setGo(String go) {
        this.go = go;
        return this;
    }

    public String getExpression() {
        return expression;
    }

    public ZettaInterpretationAnalysisParams setExpression(String expression) {
        this.expression = expression;
        return this;
    }

    public String getProteinKeyword() {
        return proteinKeyword;
    }

    public ZettaInterpretationAnalysisParams setProteinKeyword(String proteinKeyword) {
        this.proteinKeyword = proteinKeyword;
        return this;
    }

    public String getDrug() {
        return drug;
    }

    public ZettaInterpretationAnalysisParams setDrug(String drug) {
        this.drug = drug;
        return this;
    }

    public String getFunctionalScore() {
        return functionalScore;
    }

    public ZettaInterpretationAnalysisParams setFunctionalScore(String functionalScore) {
        this.functionalScore = functionalScore;
        return this;
    }

    public String getClinical() {
        return clinical;
    }

    public ZettaInterpretationAnalysisParams setClinical(String clinical) {
        this.clinical = clinical;
        return this;
    }

    public String getClinicalSignificance() {
        return clinicalSignificance;
    }

    public ZettaInterpretationAnalysisParams setClinicalSignificance(String clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
        return this;
    }

    public boolean getClinicalConfirmedStatus() {
        return clinicalConfirmedStatus;
    }

    public ZettaInterpretationAnalysisParams setClinicalConfirmedStatus(boolean clinicalConfirmedStatus) {
        this.clinicalConfirmedStatus = clinicalConfirmedStatus;
        return this;
    }


    public String getCustomAnnotation() {
        return customAnnotation;
    }

    public ZettaInterpretationAnalysisParams setCustomAnnotation(String customAnnotation) {
        this.customAnnotation = customAnnotation;
        return this;
    }

    public String getPanel() {
        return panel;
    }

    public ZettaInterpretationAnalysisParams setPanel(String panel) {
        this.panel = panel;
        return this;
    }

    public String getPanelModeOfInheritance() {
        return panelModeOfInheritance;
    }

    public ZettaInterpretationAnalysisParams setPanelModeOfInheritance(String panelModeOfInheritance) {
        this.panelModeOfInheritance = panelModeOfInheritance;
        return this;
    }

    public String getPanelConfidence() {
        return panelConfidence;
    }

    public ZettaInterpretationAnalysisParams setPanelConfidence(String panelConfidence) {
        this.panelConfidence = panelConfidence;
        return this;
    }

    public String getPanelRoleInCancer() {
        return panelRoleInCancer;
    }

    public ZettaInterpretationAnalysisParams setPanelRoleInCancer(String panelRoleInCancer) {
        this.panelRoleInCancer = panelRoleInCancer;
        return this;
    }

    public String getTrait() {
        return trait;
    }

    public ZettaInterpretationAnalysisParams setTrait(String trait) {
        this.trait = trait;
        return this;
    }

    public boolean isPrimary() {
        return primary;
    }

    public ZettaInterpretationAnalysisParams setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }
}
