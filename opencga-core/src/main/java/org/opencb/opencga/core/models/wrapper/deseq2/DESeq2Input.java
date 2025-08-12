package org.opencb.opencga.core.models.wrapper.deseq2;

public class DESeq2Input {

    private String countsFile;
    private String metadataFile;

    public DESeq2Input() {
    }

    public DESeq2Input(String countsFile, String metadataFile) {
        this.countsFile = countsFile;
        this.metadataFile = metadataFile;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DESeq2Input{");
        sb.append("countsFile='").append(countsFile).append('\'');
        sb.append(", metadataFile='").append(metadataFile).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getCountsFile() {
        return countsFile;
    }

    public DESeq2Input setCountsFile(String countsFile) {
        this.countsFile = countsFile;
        return this;
    }

    public String getMetadataFile() {
        return metadataFile;
    }

    public DESeq2Input setMetadataFile(String metadataFile) {
        this.metadataFile = metadataFile;
        return this;
    }
}