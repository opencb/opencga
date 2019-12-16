package org.opencb.opencga.core.api.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantFamilyIndexParams extends ToolParams {

    public static final String DESCRIPTION = "Variant family index params.";
    private List<String> family;
    private boolean overwrite;
    private boolean skipIncompleteFamilies;

    public VariantFamilyIndexParams() {
    }

    public VariantFamilyIndexParams(List<String> family, boolean overwrite, boolean skipIncompleteFamilies) {
        this.family = family;
        this.overwrite = overwrite;
        this.skipIncompleteFamilies = skipIncompleteFamilies;
    }

    public List<String> getFamily() {
        return family;
    }

    public VariantFamilyIndexParams setFamily(List<String> family) {
        this.family = family;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public VariantFamilyIndexParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public boolean isSkipIncompleteFamilies() {
        return skipIncompleteFamilies;
    }

    public void setSkipIncompleteFamilies(boolean skipIncompleteFamilies) {
        this.skipIncompleteFamilies = skipIncompleteFamilies;
    }
}
