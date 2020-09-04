package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class ZettaInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Zetta interpretation analysis params";

    private String clinicalAnalysis;

    // Variant filters
    private List<String> id;
    private String region;
    private String type;

    // Study filters
    private String study;
    private String file;
    private String filter;
    private String qual;
    private String fileData;

    private String sample;
    private String sampleData;
    private String sampleAnnotation;
    private String sampleMetadata;

    private String cohort;
    private String cohortStatsRef;
    private String cohortStatsAlt;
    private String cohortStatsMaf;
    private String cohortStatsMgf;
    private String cohortStatsPass;
    private String score;

    private String family;
    private String familyDisorder;
    private String familySegregation;
    private String familyMembers;
    private String familyProband;

    // Annotation filters
    private String gene;
    private String ct;
    private String xref;
    private String biotype;
    private String proteinSubstitution;
    private String conservation;
    private String populationFrequencyAlt;
    private String populationFrequencyRef;
    private String populationFrequencyMaf;
    private String transcriptFlag;
    private String geneTraitId;
    private String go;
    private String expression;
    private String proteinKeyword;
    private String drug;
    private String functionalScore;
    private String clinicalSignificance;
    private String customAnnotation;

    private String panel;

    private String trait;

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

    public String getClinicalSignificance() {
        return clinicalSignificance;
    }

    public ZettaInterpretationAnalysisParams setClinicalSignificance(String clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
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
