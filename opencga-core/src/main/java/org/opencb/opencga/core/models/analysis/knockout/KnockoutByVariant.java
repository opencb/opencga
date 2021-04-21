package org.opencb.opencga.core.models.analysis.knockout;

import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.List;

public class KnockoutByVariant {

    private String id;
    private String dbSnp;
    private String chromosome;
    private int start;
    private int end;
    private int length;
    private String reference;
    private String alternate;
    private VariantType type;

    private List<PopulationFrequency> populationFrequencies;
    private List<ClinicalSignificance> clinicalSignificance;

    private int numIndividuals;
    private boolean hasNextIndividual;
    private List<KnockoutByIndividual> individuals;

    public KnockoutByVariant() {
    }

    public KnockoutByVariant(String id, List<KnockoutByIndividual> individuals) {
        this(id, "", null, -1, -1, 0, null, null, null, null, null, individuals, false);
    }

    public KnockoutByVariant(String id, String chromosome, int start, int end, int length, String reference, String alternate,
                             List<KnockoutByIndividual> individuals) {
        this(id, "", chromosome, start, end, length, reference, alternate, null, null, null, individuals, false);
    }

    public KnockoutByVariant(String id, String dbSnp, String chromosome, int start, int end, int length, String reference, String alternate,
                             VariantType type, List<PopulationFrequency> populationFrequencies,
                             List<ClinicalSignificance> clinicalSignificance, List<KnockoutByIndividual> individuals,
                             boolean hasNextIndividual) {
        this.id = id;
        this.dbSnp = dbSnp;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.length = length;
        this.reference = reference;
        this.alternate = alternate;
        this.type = type;
        this.populationFrequencies = populationFrequencies;
        this.clinicalSignificance = clinicalSignificance;
        this.numIndividuals = individuals != null ? individuals.size() : 0;
        this.individuals = individuals;
        this.hasNextIndividual = hasNextIndividual;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutByVariant{");
        sb.append("id='").append(id).append('\'');
        sb.append(", dbSnp='").append(dbSnp).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", length=").append(length);
        sb.append(", reference='").append(reference).append('\'');
        sb.append(", alternate='").append(alternate).append('\'');
        sb.append(", type=").append(type);
        sb.append(", populationFrequencies=").append(populationFrequencies);
        sb.append(", clinicalSignificance=").append(clinicalSignificance);
        sb.append(", numIndividuals=").append(numIndividuals);
        sb.append(", hasNextIndividual=").append(hasNextIndividual);
        sb.append(", individuals=").append(individuals);
        sb.append('}');
        return sb.toString();
    }

    public KnockoutByVariant setVariantFields(KnockoutVariant knockoutVariant) {
        this.dbSnp = knockoutVariant.getDbSnp();
        this.chromosome = knockoutVariant.getChromosome();
        this.start = knockoutVariant.getStart();
        this.end = knockoutVariant.getEnd();
        this.length = knockoutVariant.getLength();
        this.reference = knockoutVariant.getReference();
        this.alternate = knockoutVariant.getAlternate();
        this.type = knockoutVariant.getType();
        this.populationFrequencies = knockoutVariant.getPopulationFrequencies();
        this.clinicalSignificance = knockoutVariant.getClinicalSignificance();
        return this;
    }

    public String getId() {
        return id;
    }

    public KnockoutByVariant setId(String id) {
        this.id = id;
        return this;
    }

    public String getDbSnp() {
        return dbSnp;
    }

    public KnockoutByVariant setDbSnp(String dbSnp) {
        this.dbSnp = dbSnp;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public KnockoutByVariant setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public KnockoutByVariant setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public KnockoutByVariant setEnd(int end) {
        this.end = end;
        return this;
    }

    public int getLength() {
        return length;
    }

    public KnockoutByVariant setLength(int length) {
        this.length = length;
        return this;
    }

    public String getReference() {
        return reference;
    }

    public KnockoutByVariant setReference(String reference) {
        this.reference = reference;
        return this;
    }

    public String getAlternate() {
        return alternate;
    }

    public KnockoutByVariant setAlternate(String alternate) {
        this.alternate = alternate;
        return this;
    }

    public VariantType getType() {
        return type;
    }

    public KnockoutByVariant setType(VariantType type) {
        this.type = type;
        return this;
    }

    public List<PopulationFrequency> getPopulationFrequencies() {
        return populationFrequencies;
    }

    public KnockoutByVariant setPopulationFrequencies(List<PopulationFrequency> populationFrequencies) {
        this.populationFrequencies = populationFrequencies;
        return this;
    }

    public List<ClinicalSignificance> getClinicalSignificance() {
        return clinicalSignificance;
    }

    public KnockoutByVariant setClinicalSignificance(List<ClinicalSignificance> clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
        return this;
    }

    public int getNumIndividuals() {
        return numIndividuals;
    }

    public KnockoutByVariant setNumIndividuals(int numIndividuals) {
        this.numIndividuals = numIndividuals;
        return this;
    }

    public boolean isHasNextIndividual() {
        return hasNextIndividual;
    }

    public KnockoutByVariant setHasNextIndividual(boolean hasNextIndividual) {
        this.hasNextIndividual = hasNextIndividual;
        return this;
    }

    public List<KnockoutByIndividual> getIndividuals() {
        return individuals;
    }

    public KnockoutByVariant setIndividuals(List<KnockoutByIndividual> individuals) {
        this.individuals = individuals;
        return this;
    }
}
