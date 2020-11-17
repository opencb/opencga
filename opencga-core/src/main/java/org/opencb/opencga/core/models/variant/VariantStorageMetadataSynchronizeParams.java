package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantStorageMetadataSynchronizeParams extends ToolParams {
    public static final String DESCRIPTION = "Variant storage metadata synchronize params.";

    private String study;

    private List<String> files;

    public String getStudy() {
        return study;
    }

    public VariantStorageMetadataSynchronizeParams setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getFiles() {
        return files;
    }

    public VariantStorageMetadataSynchronizeParams setFiles(List<String> files) {
        this.files = files;
        return this;
    }
}
