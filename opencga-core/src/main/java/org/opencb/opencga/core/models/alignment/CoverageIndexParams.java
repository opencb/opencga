package org.opencb.opencga.core.models.alignment;


import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.opencga.core.tools.ToolParams;

public class CoverageIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Coverage computation parameters";

    private String bamFileId;
    private String baiFileId;

    @JsonProperty(defaultValue = "1")
    private int windowSize;

    public CoverageIndexParams() {
    }

    public CoverageIndexParams(String bamFileId, String baiFileId, int windowSize) {
        this.bamFileId = bamFileId;
        this.baiFileId = baiFileId;
        this.windowSize = windowSize;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CoverageIndexParams{");
        sb.append("bamFileId='").append(bamFileId).append('\'');
        sb.append(", baiFileId='").append(baiFileId).append('\'');
        sb.append(", windowSize=").append(windowSize);
        sb.append('}');
        return sb.toString();
    }

    public String getBamFileId() {
        return bamFileId;
    }

    public CoverageIndexParams setBamFileId(String bamFileId) {
        this.bamFileId = bamFileId;
        return this;
    }

    public String getBaiFileId() {
        return baiFileId;
    }

    public CoverageIndexParams setBaiFileId(String baiFileId) {
        this.baiFileId = baiFileId;
        return this;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public CoverageIndexParams setWindowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }
}
