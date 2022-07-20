package org.opencb.opencga.core.models.analysis.knockout;

import java.util.Map;
import java.util.Set;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class KnockoutByGeneSummary {

    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_SUMMARY_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_SUMMARY_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_SUMMARY_CHROMOSOME_DESCRIPTION)
    private String chromosome;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_SUMMARY_START_DESCRIPTION)
    private int start;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_SUMMARY_END_DESCRIPTION)
    private int end;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_SUMMARY_STRAND_DESCRIPTION)
    private String strand;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_SUMMARY_BIOTYPE_DESCRIPTION)
    private String biotype;

    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_SUMMARY_INDIVIDUAL_STATS_DESCRIPTION)
    private IndividualKnockoutStats individualStats;
    @DataField(description = ParamConstants.KNOCKOUT_BY_GENE_SUMMARY_VARIANT_STATS_DESCRIPTION)
    private VariantKnockoutStats variantStats;

    public KnockoutByGeneSummary() {
    }

    public KnockoutByGeneSummary(String id, String name, String chromosome, int start, int end, String strand, String biotype,
                                 IndividualKnockoutStats individualStats, VariantKnockoutStats variantStats) {
        this.id = id;
        this.name = name;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.strand = strand;
        this.biotype = biotype;
        this.individualStats = individualStats;
        this.variantStats = variantStats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KnockoutByGeneSummary{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", chromosome='").append(chromosome).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", strand='").append(strand).append('\'');
        sb.append(", biotype='").append(biotype).append('\'');
        sb.append(", individualStats=").append(individualStats);
        sb.append(", variantStats=").append(variantStats);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public KnockoutByGeneSummary setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public KnockoutByGeneSummary setName(String name) {
        this.name = name;
        return this;
    }

    public String getChromosome() {
        return chromosome;
    }

    public KnockoutByGeneSummary setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getStart() {
        return start;
    }

    public KnockoutByGeneSummary setStart(int start) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return end;
    }

    public KnockoutByGeneSummary setEnd(int end) {
        this.end = end;
        return this;
    }

    public String getStrand() {
        return strand;
    }

    public KnockoutByGeneSummary setStrand(String strand) {
        this.strand = strand;
        return this;
    }

    public String getBiotype() {
        return biotype;
    }

    public KnockoutByGeneSummary setBiotype(String biotype) {
        this.biotype = biotype;
        return this;
    }

    public IndividualKnockoutStats getIndividualStats() {
        return individualStats;
    }

    public KnockoutByGeneSummary setIndividualStats(IndividualKnockoutStats individualStats) {
        this.individualStats = individualStats;
        return this;
    }

    public VariantKnockoutStats getVariantStats() {
        return variantStats;
    }

    public KnockoutByGeneSummary setVariantStats(VariantKnockoutStats variantStats) {
        this.variantStats = variantStats;
        return this;
    }

}
