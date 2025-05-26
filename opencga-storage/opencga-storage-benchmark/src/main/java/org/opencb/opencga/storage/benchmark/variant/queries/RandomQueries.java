package org.opencb.opencga.storage.benchmark.variant.queries;

import org.opencb.biodata.models.core.Region;

import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 31/10/18.
 */
public class RandomQueries {

    private List<Region> regions;
    private List<String> gene;
    private String geneFile;
    private List<String> ct;
    private List<String> type;
    private List<String> study;
    private List<String> file;
    private List<String> sample;
    private List<String> filter;
    private List<String> biotype;
    private List<String> transcriptionFlags;
    private List<String> xref;
    private List<String> drug;
    private List<String> clinicalSignificance;
    private List<String> includeSample;
    private List<String> includeFile;
    private List<String> includeStudy;

    private Score qual;
    private Score conservation;
    private List<Score> proteinSubstitution;
    private List<Score> populationFrequencies;
    private List<Score> functionalScore;

    private List<String> sessionIds;
    private Map<String, String> baseQuery;

    public RandomQueries() {
    }

    public Map<String, String> getBaseQuery() {
        return baseQuery;
    }

    public RandomQueries setBaseQuery(Map<String, String> baseQuery) {
        this.baseQuery = baseQuery;
        return this;
    }

    public List<String> getSessionIds() {
        return sessionIds;
    }

    public RandomQueries setSessionIds(List<String> sessionIds) {
        this.sessionIds = sessionIds;
        return this;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public RandomQueries setRegions(List<Region> regions) {
        this.regions = regions;
        return this;
    }

    public List<String> getGene() {
        return gene;
    }

    public RandomQueries setGene(List<String> gene) {
        this.gene = gene;
        return this;
    }

    public String getGeneFile() {
        return geneFile;
    }

    public RandomQueries setGeneFile(String geneFile) {
        this.geneFile = geneFile;
        return this;
    }

    public List<String> getCt() {
        return ct;
    }

    public RandomQueries setCt(List<String> ct) {
        this.ct = ct;
        return this;
    }

    public List<String> getType() {
        return type;
    }

    public RandomQueries setType(List<String> type) {
        this.type = type;
        return this;
    }

    public List<String> getStudy() {
        return study;
    }

    public RandomQueries setStudy(List<String> study) {
        this.study = study;
        return this;
    }

    public List<String> getFile() {
        return file;
    }

    public RandomQueries setFile(List<String> file) {
        this.file = file;
        return this;
    }

    public List<String> getSample() {
        return sample;
    }

    public RandomQueries setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public List<String> getFilter() {
        return filter;
    }

    public RandomQueries setFilter(List<String> filter) {
        this.filter = filter;
        return this;
    }

    public List<String> getBiotype() {
        return biotype;
    }

    public RandomQueries setBiotype(List<String> biotype) {
        this.biotype = biotype;
        return this;
    }

    public List<String> getTranscriptionFlags() {
        return transcriptionFlags;
    }

    public RandomQueries setTranscriptionFlags(List<String> transcriptionFlags) {
        this.transcriptionFlags = transcriptionFlags;
        return this;
    }

    public List<String> getXref() {
        return xref;
    }

    public RandomQueries setXref(List<String> xref) {
        this.xref = xref;
        return this;
    }

    public List<String> getDrug() {
        return drug;
    }

    public RandomQueries setDrug(List<String> drug) {
        this.drug = drug;
        return this;
    }

    public List<String> getClinicalSignificance() {
        return clinicalSignificance;
    }

    public RandomQueries setClinicalSignificance(List<String> clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
        return this;
    }

    public Score getQual() {
        return qual;
    }

    public RandomQueries setQual(Score qual) {
        this.qual = qual;
        return this;
    }

    public Score getConservation() {
        return conservation;
    }

    public RandomQueries setConservation(Score conservation) {
        this.conservation = conservation;
        return this;
    }

    public List<Score> getProteinSubstitution() {
        return proteinSubstitution;
    }

    public RandomQueries setProteinSubstitution(List<Score> proteinSubstitution) {
        this.proteinSubstitution = proteinSubstitution;
        return this;
    }

    public List<Score> getPopulationFrequencies() {
        return populationFrequencies;
    }

    public RandomQueries setPopulationFrequencies(List<Score> populationFrequencies) {
        this.populationFrequencies = populationFrequencies;
        return this;
    }

    public List<Score> getFunctionalScore() {
        return functionalScore;
    }

    public RandomQueries setFunctionalScore(List<Score> functionalScore) {
        this.functionalScore = functionalScore;
        return this;
    }

    public List<String> getIncludeSample() {
        return includeSample;
    }

    public RandomQueries setIncludeSample(List<String> includeSample) {
        this.includeSample = includeSample;
        return this;
    }

    public List<String> getIncludeFile() {
        return includeFile;
    }

    public RandomQueries setIncludeFile(List<String> includeFile) {
        this.includeFile = includeFile;
        return this;
    }

    public List<String> getIncludeStudy() {
        return includeStudy;
    }

    public RandomQueries setIncludeStudy(List<String> includeStudy) {
        this.includeStudy = includeStudy;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RandomQueries{");
        sb.append("regions=").append(regions);
        sb.append(", gene=").append(gene);
        sb.append(", ct=").append(ct);
        sb.append(", type=").append(type);
        sb.append(", study=").append(study);
        sb.append(", file=").append(file);
        sb.append(", sample=").append(sample);
        sb.append(", filter=").append(filter);
        sb.append(", biotype=").append(biotype);
        sb.append(", transcriptionFlags=").append(transcriptionFlags);
        sb.append(", xref=").append(xref);
        sb.append(", drug=").append(drug);
        sb.append(", clinicalSignificance=").append(clinicalSignificance);
        sb.append(", includeSample=").append(includeSample);
        sb.append(", includeFile=").append(includeFile);
        sb.append(", includeStudy=").append(includeStudy);
        sb.append(", qual=").append(qual);
        sb.append(", conservation=").append(conservation);
        sb.append(", proteinSubstitution=").append(proteinSubstitution);
        sb.append(", populationFrequencies=").append(populationFrequencies);
        sb.append(", functionalScore=").append(functionalScore);
        sb.append(", sessionIds=").append(sessionIds);
        sb.append(", baseQuery=").append(baseQuery);
        sb.append('}');
        return sb.toString();
    }
}
