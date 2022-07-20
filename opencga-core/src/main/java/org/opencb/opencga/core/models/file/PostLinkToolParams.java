package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class PostLinkToolParams extends ToolParams {

    public static final String DESCRIPTION = "File postlink params";

    @DataField(description = ParamConstants.POST_LINK_TOOL_PARAMS_FILES_DESCRIPTION)
    private List<String> files;
    @DataField(description = ParamConstants.POST_LINK_TOOL_PARAMS_BATCH_SIZE_DESCRIPTION)
    private Integer batchSize;

    public PostLinkToolParams() {
    }

    public PostLinkToolParams(List<String> files, Integer batchSize) {
        this.files = files;
        this.batchSize = batchSize;
    }

    public List<String> getFiles() {
        return files;
    }

    public PostLinkToolParams setFiles(List<String> files) {
        this.files = files;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public PostLinkToolParams setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }
}
