package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantFileIndexJobLauncherParams extends ToolParams {

    public static final String DESCRIPTION = "";
    private String name;
    private String directory;
    private boolean resumeFailed;
    private boolean ignoreFailed;
    private int maxJobs = -1;
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
