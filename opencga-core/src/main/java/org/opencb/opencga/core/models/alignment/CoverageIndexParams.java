package org.opencb.opencga.core.models.alignment;


import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class CoverageIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Coverage computation parameters";

    @DataField(id = "bamFileId", description = FieldConstants.COVERAGE_INDEX_BAM_FILE_ID_DESCRIPTION, required = true)
    private String bamFileId;

    @DataField(id = "baiFileId", description = FieldConstants.COVERAGE_INDEX_BAI_FILE_ID_DESCRIPTION)
    private String baiFileId;

    @DataField(id = "windowSize", description = FieldConstants.COVERAGE_INDEX_OVERWRITE_DESCRIPTION,
            defaultValue = ParamConstants.COVERAGE_WINDOW_SIZE_DEFAULT)
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
