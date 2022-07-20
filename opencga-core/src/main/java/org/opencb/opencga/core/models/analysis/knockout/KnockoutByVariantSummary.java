package org.opencb.opencga.core.models.analysis.knockout;

import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.List;
import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class KnockoutByVariantSummary {

    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_DB_SNP_DESCRIPTION)
    private String dbSnp;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_CHROMOSOME_DESCRIPTION)
    private String chromosome;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_START_DESCRIPTION)
    private int start;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_END_DESCRIPTION)
    private int end;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_LENGTH_DESCRIPTION)
    private int length;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_REFERENCE_DESCRIPTION)
    private String reference;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_ALTERNATE_DESCRIPTION)
    private String alternate;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_TYPE_DESCRIPTION)
    private VariantType type;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_GENES_DESCRIPTION)
    private List<String> genes;

    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_POPULATION_FREQUENCIES_DESCRIPTION)
    private List<PopulationFrequency> populationFrequencies;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_SEQUENCE_ONTOLOGY_TERMS_DESCRIPTION)
    private List<SequenceOntologyTerm> sequenceOntologyTerms;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_CLINICAL_SIGNIFICANCES_DESCRIPTION)
    private List<ClinicalSignificance> clinicalSignificances;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_ALLELE_PAIRS_DESCRIPTION)
    private List<KnockoutVariant> allelePairs;

    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_INDIVIDUAL_STATS_DESCRIPTION)
    private IndividualKnockoutStats individualStats;
    @DataField(description = ParamConstants.KNOCKOUT_BY_VARIANT_SUMMARY_TRANSCRIPT_CH_PAIRS_DESCRIPTION)
    private Map<String, List<String>> transcriptChPairs;

    public KnockoutByVariantSummary() {
    }

    public KnockoutByVariantSummary(String id, String dbSnp, String chromosome, int start, int end, int length, String reference,
                                    String alternate, VariantType type, List<String> genes, List<PopulationFrequency> populationFrequencies,
                                    List<SequenceOntologyTerm> sequenceOntologyTerms, List<ClinicalSignificance> clinicalSignificances,
                                    List<KnockoutVariant> allelePairs, IndividualKnockoutStats individualStats,
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

    public IndividualKnockoutStats getIndividualStats() {
        return individualStats;
    }

    public KnockoutByVariantSummary setIndividualStats(IndividualKnockoutStats individualStats) {
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
