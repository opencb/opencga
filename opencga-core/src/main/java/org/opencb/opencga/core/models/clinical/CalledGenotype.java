package org.opencb.opencga.core.models.clinical;

@Deprecated
public class CalledGenotype {

    private long sampleId;
    private long individualId;

    private String genotype;
    private String zigosity;

    private int phaseSet;
    private int depthReference;
    private int depthAlternate;
    private int copyNumber;

    public CalledGenotype() {
    }

    public CalledGenotype(long sampleId, long individualId, String genotype, String zigosity, int phaseSet, int depthReference,
                          int depthAlternate, int copyNumber) {
        this.sampleId = sampleId;
        this.individualId = individualId;
        this.genotype = genotype;
        this.zigosity = zigosity;
        this.phaseSet = phaseSet;
        this.depthReference = depthReference;
        this.depthAlternate = depthAlternate;
        this.copyNumber = copyNumber;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CalledGenotype{");
        sb.append("sampleId=").append(sampleId);
        sb.append(", individualId=").append(individualId);
        sb.append(", genotype='").append(genotype).append('\'');
        sb.append(", zigosity='").append(zigosity).append('\'');
        sb.append(", phaseSet=").append(phaseSet);
        sb.append(", depthReference=").append(depthReference);
        sb.append(", depthAlternate=").append(depthAlternate);
        sb.append(", copyNumber=").append(copyNumber);
        sb.append('}');
        return sb.toString();
    }

    public long getSampleId() {
        return sampleId;
    }

    public CalledGenotype setSampleId(long sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public long getIndividualId() {
        return individualId;
    }

    public CalledGenotype setIndividualId(long individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getGenotype() {
        return genotype;
    }

    public CalledGenotype setGenotype(String genotype) {
        this.genotype = genotype;
        return this;
    }

    public String getZigosity() {
        return zigosity;
    }

    public CalledGenotype setZigosity(String zigosity) {
        this.zigosity = zigosity;
        return this;
    }

    public int getPhaseSet() {
        return phaseSet;
    }

    public CalledGenotype setPhaseSet(int phaseSet) {
        this.phaseSet = phaseSet;
        return this;
    }

    public int getDepthReference() {
        return depthReference;
    }

    public CalledGenotype setDepthReference(int depthReference) {
        this.depthReference = depthReference;
        return this;
    }

    public int getDepthAlternate() {
        return depthAlternate;
    }

    public CalledGenotype setDepthAlternate(int depthAlternate) {
        this.depthAlternate = depthAlternate;
        return this;
    }

    public int getCopyNumber() {
        return copyNumber;
    }

    public CalledGenotype setCopyNumber(int copyNumber) {
        this.copyNumber = copyNumber;
        return this;
    }

}
