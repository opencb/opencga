package org.opencb.opencga.core.models.variant;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.tools.ToolParams;

public class AbstractBasicVariantQueryParams extends ToolParams {
    private String id;
    private String region;
    private String gene;
    private String type;
    private String panel;
    private String cohortStatsRef;
    private String cohortStatsAlt;
    private String cohortStatsMaf;
    private String ct;
    private String xref;
    private String biotype;
    private String proteinSubstitution;
    private String conservation;
    private String populationFrequencyMaf;
    private String populationFrequencyAlt;
    private String populationFrequencyRef;
    private String transcriptFlag;
    private String functionalScore;
    private String clinicalSignificance;

    public Query toQuery() {
        return new Query(toObjectMap());
    }

    public String getId() {
        return id;
    }

    public AbstractBasicVariantQueryParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public AbstractBasicVariantQueryParams setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getGene() {
        return gene;
    }

    public AbstractBasicVariantQueryParams setGene(String gene) {
        this.gene = gene;
        return this;
    }

    public String getType() {
        return type;
    }

    public AbstractBasicVariantQueryParams setType(String type) {
        this.type = type;
        return this;
    }

    public String getPanel() {
        return panel;
    }

    public AbstractBasicVariantQueryParams setPanel(String panel) {
        this.panel = panel;
        return this;
    }

    public String getCohortStatsRef() {
        return cohortStatsRef;
    }

    public AbstractBasicVariantQueryParams setCohortStatsRef(String cohortStatsRef) {
        this.cohortStatsRef = cohortStatsRef;
        return this;
    }

    public String getCohortStatsAlt() {
        return cohortStatsAlt;
    }

    public AbstractBasicVariantQueryParams setCohortStatsAlt(String cohortStatsAlt) {
        this.cohortStatsAlt = cohortStatsAlt;
        return this;
    }

    public String getCohortStatsMaf() {
        return cohortStatsMaf;
    }

    public AbstractBasicVariantQueryParams setCohortStatsMaf(String cohortStatsMaf) {
        this.cohortStatsMaf = cohortStatsMaf;
        return this;
    }

    public String getCt() {
        return ct;
    }

    public AbstractBasicVariantQueryParams setCt(String ct) {
        this.ct = ct;
        return this;
    }

    public String getXref() {
        return xref;
    }

    public AbstractBasicVariantQueryParams setXref(String xref) {
        this.xref = xref;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public AbstractBasicVariantQueryParams setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public String getProteinSubstitution() {
        return proteinSubstitution;
    }

    public AbstractBasicVariantQueryParams setProteinSubstitution(String proteinSubstitution) {
        this.proteinSubstitution = proteinSubstitution;
        return this;
    }

    public String getConservation() {
        return conservation;
    }

    public AbstractBasicVariantQueryParams setConservation(String conservation) {
        this.conservation = conservation;
        return this;
    }

    public String getPopulationFrequencyMaf() {
        return populationFrequencyMaf;
    }

    public AbstractBasicVariantQueryParams setPopulationFrequencyMaf(String populationFrequencyMaf) {
        this.populationFrequencyMaf = populationFrequencyMaf;
        return this;
    }

    public String getPopulationFrequencyAlt() {
        return populationFrequencyAlt;
    }

    public AbstractBasicVariantQueryParams setPopulationFrequencyAlt(String populationFrequencyAlt) {
        this.populationFrequencyAlt = populationFrequencyAlt;
        return this;
    }

    public String getPopulationFrequencyRef() {
        return populationFrequencyRef;
    }

    public AbstractBasicVariantQueryParams setPopulationFrequencyRef(String populationFrequencyRef) {
        this.populationFrequencyRef = populationFrequencyRef;
        return this;
    }

    public String getTranscriptFlag() {
        return transcriptFlag;
    }

    public AbstractBasicVariantQueryParams setTranscriptFlag(String transcriptFlag) {
        this.transcriptFlag = transcriptFlag;
        return this;
    }

    public String getFunctionalScore() {
        return functionalScore;
    }

    public AbstractBasicVariantQueryParams setFunctionalScore(String functionalScore) {
        this.functionalScore = functionalScore;
        return this;
    }

    public String getClinicalSignificance() {
        return clinicalSignificance;
    }

    public AbstractBasicVariantQueryParams setClinicalSignificance(String clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
        return this;
    }
}
