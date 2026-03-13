package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single variant-level location entry from the CPIC allele_definition endpoint.
 * Represents one element of the {@code allele_location_value} array.
 */
public class CpicAlleleLocationValue {

    private String variantallele;          // e.g. "delA"

    @JsonProperty("sequence_location")
    private CpicSequenceLocation sequenceLocation;

    public CpicAlleleLocationValue() {
    }

    public CpicAlleleLocationValue(String variantallele, CpicSequenceLocation sequenceLocation) {
        this.variantallele = variantallele;
        this.sequenceLocation = sequenceLocation;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicAlleleLocationValue{");
        sb.append("variantallele='").append(variantallele).append('\'');
        sb.append(", sequenceLocation=").append(sequenceLocation);
        sb.append('}');
        return sb.toString();
    }

    public String getVariantallele() {
        return variantallele;
    }

    public CpicAlleleLocationValue setVariantallele(String variantallele) {
        this.variantallele = variantallele;
        return this;
    }

    public CpicSequenceLocation getSequenceLocation() {
        return sequenceLocation;
    }

    public CpicAlleleLocationValue setSequenceLocation(CpicSequenceLocation sequenceLocation) {
        this.sequenceLocation = sequenceLocation;
        return this;
    }
}
