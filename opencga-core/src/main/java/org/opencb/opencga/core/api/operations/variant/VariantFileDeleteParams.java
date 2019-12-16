package org.opencb.opencga.core.api.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantFileDeleteParams extends ToolParams {

    public VariantFileDeleteParams() {
    }

    public VariantFileDeleteParams(List<String> file, boolean resume) {
        this.file = file;
        this.resume = resume;
    }

    private List<String> file;
    private boolean resume;

    public List<String> getFile() {
        return file;
    }

    public VariantFileDeleteParams setFile(List<String> file) {
        this.file = file;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantFileDeleteParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }
}
