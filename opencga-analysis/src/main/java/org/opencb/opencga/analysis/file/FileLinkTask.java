package org.opencb.opencga.analysis.file;

import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.Collections;
import java.util.Map;

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
                    = catalogManager.getFileManager().link(getStudy(), linkParams, toolParams.isParents(), getToken());
            if (result.getEvents().stream().anyMatch(e -> e.getMessage().equals(ParamConstants.FILE_ALREADY_LINKED))) {
                logger.info("File already linked - SKIP");
            } else {
                logger.info("File link took " + TimeUtils.durationToString(stopWatch));
                addGeneratedFile(result.first());
                for (File fileResult : result.getResults()) {
                    if (fileResult.getInternal().getStatus().getId().equals(FileStatus.MISSING_SAMPLES)) {
                        Map<String, Object> params = new PostLinkToolParams(Collections.singletonList(fileResult.getId()), null)
                                .toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study));
                        Job postLinkJob = catalogManager.getJobManager()
                                .submit(getStudy(), PostLinkSampleAssociation.ID, Enums.Priority.MEDIUM,
                                        params, null, "Job generated by " + getId() + " - " + getJobId(),
                                        Collections.emptyList(), Collections.emptyList(), getToken()).first();
                        logger.info("Submit post-link job : " + postLinkJob.getId());
                    }
                }
            }
        }
    }
}