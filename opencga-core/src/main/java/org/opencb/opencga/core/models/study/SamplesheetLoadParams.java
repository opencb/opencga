package org.opencb.opencga.core.models.study;

import org.opencb.commons.annotations.DataField;

public class SamplesheetLoadParams {

    public static final String DESCRIPTION = "Samplesheet load parameters";

    @DataField(id = "samplesheetFile", description = "Catalog file ID of the samplesheet (CSV, TSV, or TXT)")
    private String samplesheetFile;

    @DataField(id = "samplesheetContent", description = "Samplesheet content as inline string")
    private String samplesheetContent;

    public SamplesheetLoadParams() {
    }

    public SamplesheetLoadParams(String samplesheetFile, String samplesheetContent) {
        this.samplesheetFile = samplesheetFile;
        this.samplesheetContent = samplesheetContent;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SamplesheetLoadParams{");
        sb.append("samplesheetFile='").append(samplesheetFile).append('\'');
        sb.append(", samplesheetContent='").append(samplesheetContent != null ? "<" + samplesheetContent.length() + " chars>" : "null");
        sb.append("'}");
        return sb.toString();
    }

    public String getSamplesheetFile() {
        return samplesheetFile;
    }

    public SamplesheetLoadParams setSamplesheetFile(String samplesheetFile) {
        this.samplesheetFile = samplesheetFile;
        return this;
    }

    public String getSamplesheetContent() {
        return samplesheetContent;
    }

    public SamplesheetLoadParams setSamplesheetContent(String samplesheetContent) {
        this.samplesheetContent = samplesheetContent;
        return this;
    }
}
