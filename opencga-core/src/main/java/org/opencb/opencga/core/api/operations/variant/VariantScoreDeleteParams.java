package org.opencb.opencga.core.api.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantScoreDeleteParams extends ToolParams {

    public static final String ID = "variant-score-delete";
    public static final String DESCRIPTION = "Remove a variant score in the database";
    private String scoreName;
    private boolean force;
    private boolean resume;

    public VariantScoreDeleteParams() {
    }

    public VariantScoreDeleteParams(String scoreName, boolean force, boolean resume) {
        this.scoreName = scoreName;
        this.force = force;
        this.resume = resume;
    }

    public String getScoreName() {
        return scoreName;
    }

    public VariantScoreDeleteParams setScoreName(String scoreName) {
        this.scoreName = scoreName;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public VariantScoreDeleteParams setForce(boolean force) {
        this.force = force;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantScoreDeleteParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }
}
