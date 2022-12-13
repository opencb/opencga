package org.opencb.opencga.core.models.analysis.knockout;

import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.List;
import java.util.Map;

public class KnockoutByVariantSummary {

    private String id;
    private String dbSnp;
    private String chromosome;
    private int start;
    private int end;
    private int length;
    private String reference;
    private String alternate;
    private VariantType type;
    private List<String> genes;

    private List<PopulationFrequency> populationFrequencies;
    private List<SequenceOntologyTerm> sequenceOntologyTerms;
    private List<ClinicalSignificance> clinicalSignificances;
    private List<KnockoutVariant> allelePairs;

    private GlobalIndividualKnockoutStats individualStats;
    private Map<String, List<String>> transcriptChPairs;

    public KnockoutByVariantSummary() {
    }

    public KnockoutByVariantSummary(String id, String dbSnp, String chromosome, int start, int end, int length, String reference,
                                    String alternate, VariantType type, List<String> genes, List<PopulationFrequency> populationFrequencies,
                                    List<SequenceOntologyTerm> sequenceOntologyTerms, List<ClinicalSignificance> clinicalSignificances,
                                    List<KnockoutVariant> allelePairs, GlobalIndividualKnockoutStats individualStats,
                                    Map<String, List<String>> transcriptChPairs) {
        this.id = id;
        this.dbSnp = dbSnp;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.length = length;
        this.reference = reference;
        this.alternate = alternate;
        this.type = type;
        this.genes = genes;
        this.populationFrequencies = populationFrequencies;
        this.sequenceOntologyTerms = sequenceOntologyTerms;
        this.clinicalSignificances = clinicalSignificances;
        this.allelePairs = allelePairs;
        this.individualStats = individualStats;
        this.transcriptChPairs = transcriptChPairs;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutByVariantSummary{");
        sb.append("id='").append(id).append('\'');
        sb.append(", dbSnp='").append(dbSnp).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", length=").append(length);
        sb.append(", reference='").append(reference).append('\'');
        sb.append(", alternate='").append(alternate).append('\'');
        sb.append(", type=").append(type);
        sb.append(", genes=").append(genes);
        sb.append(", populationFrequencies=").append(populationFrequencies);
        sb.append(", sequenceOntologyTerms=").append(sequenceOntologyTerms);
        sb.append(", clinicalSignificances=").append(clinicalSignificances);
        sb.append(", allelePairs=").append(allelePairs);
        sb.append(", individualStats=").append(individualStats);
        sb.append(", transcriptChPairs=").append(transcriptChPairs);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public KnockoutByVariantSummary setId(String id) {
        this.id = id;
        return this;
    }

    public String getDbSnp() {
        return dbSnp;
    }

    public KnockoutByVariantSummary setDbSnp(String dbSnp) {
        this.dbSnp = dbSnp;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public KnockoutByVariantSummary setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public KnockoutByVariantSummary setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public KnockoutByVariantSummary setEnd(int end) {
        this.end = end;
        return this;
    }

    public int getLength() {
        return length;
    }

    public KnockoutByVariantSummary setLength(int length) {
        this.length = length;
        return this;
    }

    public String getReference() {
        return reference;
    }

    public KnockoutByVariantSummary setReference(String reference) {
        this.reference = reference;
        return this;
    }

    public String getAlternate() {
        return alternate;
    }

    public KnockoutByVariantSummary setAlternate(String alternate) {
        this.alternate = alternate;
        return this;
    }

    public VariantType getType() {
        return type;
    }

    public KnockoutByVariantSummary setType(VariantType type) {
        this.type = type;
        return this;
    }

    public List<String> getGenes() {
        return genes;
    }

    public KnockoutByVariantSummary setGenes(List<String> genes) {
        this.genes = genes;
        return this;
    }

    public List<PopulationFrequency> getPopulationFrequencies() {
        return populationFrequencies;
    }

    public KnockoutByVariantSummary setPopulationFrequencies(List<PopulationFrequency> populationFrequencies) {
        this.populationFrequencies = populationFrequencies;
        return this;
    }

    public List<SequenceOntologyTerm> getSequenceOntologyTerms() {
        return sequenceOntologyTerms;
    }

    public KnockoutByVariantSummary setSequenceOntologyTerms(List<SequenceOntologyTerm> sequenceOntologyTerms) {
        this.sequenceOntologyTerms = sequenceOntologyTerms;
        return this;
    }

    public List<ClinicalSignificance> getClinicalSignificances() {
        return clinicalSignificances;
    }

    public KnockoutByVariantSummary setClinicalSignificances(List<ClinicalSignificance> clinicalSignificances) {
        this.clinicalSignificances = clinicalSignificances;
        return this;
    }

    public List<KnockoutVariant> getAllelePairs() {
        return allelePairs;
    }

    public KnockoutByVariantSummary setAllelePairs(List<KnockoutVariant> allelePairs) {
        this.allelePairs = allelePairs;
        return this;
    }

    public GlobalIndividualKnockoutStats getIndividualStats() {
        return individualStats;
    }

    public KnockoutByVariantSummary setIndividualStats(GlobalIndividualKnockoutStats individualStats) {
        this.individualStats = individualStats;
        return this;
    }

    public Map<String, List<String>> getTranscriptChPairs() {
        return transcriptChPairs;
    }

    public KnockoutByVariantSummary setTranscriptChPairs(Map<String, List<String>> transcriptChPairs) {
        this.transcriptChPairs = transcriptChPairs;
        return this;
    }
}
