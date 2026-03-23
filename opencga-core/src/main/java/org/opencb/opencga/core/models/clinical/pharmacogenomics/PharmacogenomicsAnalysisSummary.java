package org.opencb.opencga.core.models.clinical.pharmacogenomics;

import java.util.List;

/**
 * Summary of pharmacogenomics analysis results.
 * Mirrors the Python PharmacogenomicsSummary model.
 */
public class PharmacogenomicsAnalysisSummary {

    private String sampleId;
    private String source;
    private String date;
    private int totalGenesAnalyzed;
    private int totalGenesWithResults;
    private int totalActionableGenes;
    private int totalDrugsAffected;
    private List<ActionableResult> actionableResults;
    private List<ActionableResult> informativeResults;
    private List<NormalResult> normalResults;
    private List<String> noTranslationGenes;
    private List<String> warnings;

    public PharmacogenomicsAnalysisSummary() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PharmacogenomicsAnalysisSummary{");
        sb.append("sampleId='").append(sampleId).append('\'');
        sb.append(", source='").append(source).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", totalGenesAnalyzed=").append(totalGenesAnalyzed);
        sb.append(", totalActionableGenes=").append(totalActionableGenes);
        sb.append(", totalDrugsAffected=").append(totalDrugsAffected);
        sb.append('}');
        return sb.toString();
    }

    public String getSampleId() { return sampleId; }
    public PharmacogenomicsAnalysisSummary setSampleId(String sampleId) { this.sampleId = sampleId; return this; }

    public String getSource() { return source; }
    public PharmacogenomicsAnalysisSummary setSource(String source) { this.source = source; return this; }

    public String getDate() { return date; }
    public PharmacogenomicsAnalysisSummary setDate(String date) { this.date = date; return this; }

    public int getTotalGenesAnalyzed() { return totalGenesAnalyzed; }
    public PharmacogenomicsAnalysisSummary setTotalGenesAnalyzed(int totalGenesAnalyzed) { this.totalGenesAnalyzed = totalGenesAnalyzed; return this; }

    public int getTotalGenesWithResults() { return totalGenesWithResults; }
    public PharmacogenomicsAnalysisSummary setTotalGenesWithResults(int totalGenesWithResults) { this.totalGenesWithResults = totalGenesWithResults; return this; }

    public int getTotalActionableGenes() { return totalActionableGenes; }
    public PharmacogenomicsAnalysisSummary setTotalActionableGenes(int totalActionableGenes) { this.totalActionableGenes = totalActionableGenes; return this; }

    public int getTotalDrugsAffected() { return totalDrugsAffected; }
    public PharmacogenomicsAnalysisSummary setTotalDrugsAffected(int totalDrugsAffected) { this.totalDrugsAffected = totalDrugsAffected; return this; }

    public List<ActionableResult> getActionableResults() { return actionableResults; }
    public PharmacogenomicsAnalysisSummary setActionableResults(List<ActionableResult> actionableResults) { this.actionableResults = actionableResults; return this; }

    public List<ActionableResult> getInformativeResults() { return informativeResults; }
    public PharmacogenomicsAnalysisSummary setInformativeResults(List<ActionableResult> informativeResults) { this.informativeResults = informativeResults; return this; }

    public List<NormalResult> getNormalResults() { return normalResults; }
    public PharmacogenomicsAnalysisSummary setNormalResults(List<NormalResult> normalResults) { this.normalResults = normalResults; return this; }

    public List<String> getNoTranslationGenes() { return noTranslationGenes; }
    public PharmacogenomicsAnalysisSummary setNoTranslationGenes(List<String> noTranslationGenes) { this.noTranslationGenes = noTranslationGenes; return this; }

    public List<String> getWarnings() { return warnings; }
    public PharmacogenomicsAnalysisSummary setWarnings(List<String> warnings) { this.warnings = warnings; return this; }

    /**
     * An actionable or informative gene result with drug recommendations.
     */
    public static class ActionableResult {
        private String gene;
        private String diplotype;
        private String renamedDiplotype;
        private String phenotype;
        private String activityScore;
        private String cpicLevel;
        private String pgkbLevel;
        private List<SummaryDrugRecommendation> drugs;

        public ActionableResult() {
        }

        public String getGene() { return gene; }
        public ActionableResult setGene(String gene) { this.gene = gene; return this; }

        public String getDiplotype() { return diplotype; }
        public ActionableResult setDiplotype(String diplotype) { this.diplotype = diplotype; return this; }

        public String getRenamedDiplotype() { return renamedDiplotype; }
        public ActionableResult setRenamedDiplotype(String renamedDiplotype) { this.renamedDiplotype = renamedDiplotype; return this; }

        public String getPhenotype() { return phenotype; }
        public ActionableResult setPhenotype(String phenotype) { this.phenotype = phenotype; return this; }

        public String getActivityScore() { return activityScore; }
        public ActionableResult setActivityScore(String activityScore) { this.activityScore = activityScore; return this; }

        public String getCpicLevel() { return cpicLevel; }
        public ActionableResult setCpicLevel(String cpicLevel) { this.cpicLevel = cpicLevel; return this; }

        public String getPgkbLevel() { return pgkbLevel; }
        public ActionableResult setPgkbLevel(String pgkbLevel) { this.pgkbLevel = pgkbLevel; return this; }

        public List<SummaryDrugRecommendation> getDrugs() { return drugs; }
        public ActionableResult setDrugs(List<SummaryDrugRecommendation> drugs) { this.drugs = drugs; return this; }
    }

    /**
     * A normal/reference gene result with no clinical impact.
     */
    public static class NormalResult {
        private String gene;
        private String diplotype;
        private String phenotype;

        public NormalResult() {
        }

        public String getGene() { return gene; }
        public NormalResult setGene(String gene) { this.gene = gene; return this; }

        public String getDiplotype() { return diplotype; }
        public NormalResult setDiplotype(String diplotype) { this.diplotype = diplotype; return this; }

        public String getPhenotype() { return phenotype; }
        public NormalResult setPhenotype(String phenotype) { this.phenotype = phenotype; return this; }
    }

    /**
     * Drug recommendation summary entry.
     */
    public static class SummaryDrugRecommendation {
        private String drugName;
        private String recommendation;
        private String classification;
        private String implications;
        private String population;

        public SummaryDrugRecommendation() {
        }

        public String getDrugName() { return drugName; }
        public SummaryDrugRecommendation setDrugName(String drugName) { this.drugName = drugName; return this; }

        public String getRecommendation() { return recommendation; }
        public SummaryDrugRecommendation setRecommendation(String recommendation) { this.recommendation = recommendation; return this; }

        public String getClassification() { return classification; }
        public SummaryDrugRecommendation setClassification(String classification) { this.classification = classification; return this; }

        public String getImplications() { return implications; }
        public SummaryDrugRecommendation setImplications(String implications) { this.implications = implications; return this; }

        public String getPopulation() { return population; }
        public SummaryDrugRecommendation setPopulation(String population) { this.population = population; return this; }
    }
}
