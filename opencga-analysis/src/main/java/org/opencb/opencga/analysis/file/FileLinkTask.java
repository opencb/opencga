package org.opencb.opencga.analysis.file;

import org.apache.commons.lang3.time.StopWatch;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileLinkToolParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = FileLinkTask.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION, description = FileLinkTask.DESCRIPTION)
public class FileLinkTask extends OpenCgaToolScopeStudy {

    public static final String ID = "file-link";
    public static final String DESCRIPTION = "Link an external file into catalog";

    @ToolParams
    protected final FileLinkToolParams toolParams = new FileLinkToolParams();

    @Override
    protected void run() throws Exception {
        for (String uri : toolParams.getUri()) {
            StopWatch stopWatch = StopWatch.createStarted();
            FileLinkParams linkParams = new FileLinkParams()
                    .setUri(uri)
                    .setPath(toolParams.getPath())
                    .setDescription(toolParams.getDescription());
            logger.info("Linking file " + uri);
            OpenCGAResult<File> result
                    = catalogManager.getFileManager().link(getStudyFqn(), linkParams, toolParams.isParents(), getToken());
            if (result.getEvents().stream().anyMatch(e -> e.getMessage().equals(ParamConstants.FILE_ALREADY_LINKED))) {
                logger.info("File already linked - SKIP");
            } else {
                logger.info("File link took " + TimeUtils.durationToString(stopWatch));
                addGeneratedFile(result.first());
            }
        }
    }
}
