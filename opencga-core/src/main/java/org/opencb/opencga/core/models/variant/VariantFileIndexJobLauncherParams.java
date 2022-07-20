package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantFileIndexJobLauncherParams extends ToolParams {

    public static final String DESCRIPTION = "";
    @DataField(description = ParamConstants.VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_DIRECTORY_DESCRIPTION)
    private String directory;
    @DataField(description = ParamConstants.VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_RESUME_FAILED_DESCRIPTION)
    private boolean resumeFailed;
    @DataField(description = ParamConstants.VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_IGNORE_FAILED_DESCRIPTION)
    private boolean ignoreFailed;
    private int maxJobs = -1;
    @DataField(description = ParamConstants.VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_INDEX_PARAMS_DESCRIPTION)
    private VariantIndexParams indexParams;

    public VariantFileIndexJobLauncherParams() {
        indexParams = new VariantIndexParams();
    }

    public VariantFileIndexJobLauncherParams(String name, String directory, boolean resumeFailed, boolean ignoreFailed, int maxJobs,
                                             VariantIndexParams indexParams) {
        this.name = name;
        this.directory = directory;
        this.resumeFailed = resumeFailed;
        this.ignoreFailed = ignoreFailed;
        this.maxJobs = maxJobs;
        this.indexParams = new VariantIndexParams();
        this.indexParams = indexParams;
    }

    public String getName() {
        return name;
    }

    public VariantFileIndexJobLauncherParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDirectory() {
        return directory;
    }

    public VariantFileIndexJobLauncherParams setDirectory(String directory) {
        this.directory = directory;
        return this;
    }

    public boolean isResumeFailed() {
        return resumeFailed;
    }

    public VariantFileIndexJobLauncherParams setResumeFailed(boolean resumeFailed) {
        this.resumeFailed = resumeFailed;
        return this;
    }

    public boolean isIgnoreFailed() {
        return ignoreFailed;
    }

    public VariantFileIndexJobLauncherParams setIgnoreFailed(boolean ignoreFailed) {
        this.ignoreFailed = ignoreFailed;
        return this;
    }

    public int getMaxJobs() {
        return maxJobs;
    }

    public VariantFileIndexJobLauncherParams setMaxJobs(int maxJobs) {
        this.maxJobs = maxJobs;
        return this;
    }

    public VariantIndexParams getIndexParams() {
        return indexParams;
    }

    public VariantFileIndexJobLauncherParams setIndexParams(VariantIndexParams indexParams) {
        this.indexParams = indexParams;
        return this;
    }
}
