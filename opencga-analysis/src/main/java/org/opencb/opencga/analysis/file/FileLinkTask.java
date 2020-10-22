package org.opencb.opencga.analysis.file;

import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileLinkToolParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = FileLinkTask.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION, description = FileLinkTask.DESCRIPTION)
public class FileLinkTask extends OpenCgaToolScopeStudy {

    public static final String ID = "file-link";
    public static final String DESCRIPTION = "";

    @ToolParams
    protected final FileLinkToolParams toolParams = new FileLinkToolParams();

    @Override
    protected void run() throws Exception {
        for (String uri : toolParams.getUri()) {
            FileLinkParams linkParams = new FileLinkParams()
                    .setUri(uri)
                    .setPath(toolParams.getPath())
                    .setDescription(toolParams.getDescription());
            catalogManager.getFileManager().link(getStudyFqn(), linkParams, toolParams.isParents(), getToken());
        }
    }
}
