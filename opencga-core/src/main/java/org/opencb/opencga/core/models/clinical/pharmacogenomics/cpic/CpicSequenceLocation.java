package org.opencb.opencga.core.models.clinical.pharmacogenomics.cpic;

/**
 * Sequence location information from the CPIC allele_definition endpoint.
 * Nested inside {@link CpicAlleleLocationValue}.
 */
public class CpicSequenceLocation {

    private String name;               // e.g. "10601delA"
    private String dbsnpid;            // e.g. "rs9332131"
    private Long position;             // chromosomal position
    private String genelocation;       // e.g. "g.16126del"
    private String proteinlocation;    // e.g. "p.K273fs"
    private String chromosomelocation; // e.g. "g.94949283del"

    public CpicSequenceLocation() {
    }

    public CpicSequenceLocation(String name, String dbsnpid, Long position, String genelocation,
                                String proteinlocation, String chromosomelocation) {
        this.name = name;
        this.dbsnpid = dbsnpid;
        this.position = position;
        this.genelocation = genelocation;
        this.proteinlocation = proteinlocation;
        this.chromosomelocation = chromosomelocation;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpicSequenceLocation{");
        sb.append("name='").append(name).append('\'');
        sb.append(", dbsnpid='").append(dbsnpid).append('\'');
        sb.append(", position=").append(position);
        sb.append(", genelocation='").append(genelocation).append('\'');
        sb.append(", proteinlocation='").append(proteinlocation).append('\'');
        sb.append(", chromosomelocation='").append(chromosomelocation).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public CpicSequenceLocation setName(String name) {
        this.name = name;
        return this;
    }

    public String getDbsnpid() {
        return dbsnpid;
    }

    public CpicSequenceLocation setDbsnpid(String dbsnpid) {
        this.dbsnpid = dbsnpid;
        return this;
    }

    public Long getPosition() {
        return position;
    }

    public CpicSequenceLocation setPosition(Long position) {
        this.position = position;
        return this;
    }

    public String getGenelocation() {
        return genelocation;
    }

    public CpicSequenceLocation setGenelocation(String genelocation) {
        this.genelocation = genelocation;
        return this;
    }

    public String getProteinlocation() {
        return proteinlocation;
    }

    public CpicSequenceLocation setProteinlocation(String proteinlocation) {
        this.proteinlocation = proteinlocation;
        return this;
    }

    public String getChromosomelocation() {
        return chromosomelocation;
    }

    public CpicSequenceLocation setChromosomelocation(String chromosomelocation) {
        this.chromosomelocation = chromosomelocation;
        return this;
    }
}
