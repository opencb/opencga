package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileIndex;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.variant.VariantFileIndexJobLauncherParams;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.*;

@Tool(id = VariantFileIndexJobLauncherTool.ID, description = VariantFileIndexJobLauncherTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantFileIndexJobLauncherTool extends OpenCgaToolScopeStudy {

    public static final String ID = "variant-index-job-launcher";
    public static final String DESCRIPTION = "Detect non-indexed VCF files in the study, and submit a job for indexing them.";

    @ToolParams
    protected final VariantFileIndexJobLauncherParams toolParams = new VariantFileIndexJobLauncherParams();

    @Override
    protected void check() throws Exception {
        super.check();
        if (StringUtils.isNotEmpty(toolParams.getDirectory())) {
            String directory = toolParams.getDirectory();
            if (!directory.startsWith("~") && !directory.endsWith("/")) {
                toolParams.setDirectory(directory + "/");
            }
        }
    }

    @Override
    protected void run() throws Exception {
        Query filesQuery = new Query()
                .append(FORMAT.key(), File.Format.VCF)
                .append(INTERNAL_INDEX_STATUS_NAME.key(), "!" + FileIndex.IndexStatus.READY);
        filesQuery.putIfNotEmpty(NAME.key(), toolParams.getName());
        filesQuery.putIfNotEmpty(DIRECTORY.key(), toolParams.getDirectory());
        QueryOptions filesInclude = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                FileDBAdaptor.QueryParams.ID.key(),
                FileDBAdaptor.QueryParams.NAME.key(),
                FileDBAdaptor.QueryParams.PATH.key(),
                FileDBAdaptor.QueryParams.INTERNAL_INDEX.key()));

        int submittedJobs = 0;
        int filesPerJob = 1;
        int maxJobs = Integer.MAX_VALUE;
        if (toolParams.getMaxJobs() > 0) {
            maxJobs = toolParams.getMaxJobs();
            logger.info("Creating up to " + maxJobs + " jobs");
        }

        int scannedFiles = 0;
        try (DBIterator<File> dbIterator = getCatalogManager().getFileManager()
                .iterator(getStudy(), filesQuery, filesInclude, getToken())) {
            while (dbIterator.hasNext() && submittedJobs != maxJobs) {
                File file = dbIterator.next();
                scannedFiles++;
                String indexStatus = getIndexStatus(file);
                OpenCGAResult<Job> jobsFromFile = getCatalogManager()
                        .getJobManager()
                        .search(getStudy(),
                                new Query()
                                        .append(JobDBAdaptor.QueryParams.INPUT.key(), file.getId())
                                        .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), VariantIndexOperationTool.ID)
                                        .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Arrays.asList(
                                                Enums.ExecutionStatus.RUNNING,
                                                Enums.ExecutionStatus.QUEUED,
                                                Enums.ExecutionStatus.PENDING
                                        )),
                                new QueryOptions(QueryOptions.INCLUDE, JobDBAdaptor.QueryParams.ID.key()),
                                getToken());

                if (jobsFromFile.getResults().isEmpty()) {
                    // The file is not indexed and it's not in any pending job.
                    VariantIndexParams indexParams = new VariantIndexParams();
                    indexParams.updateParams(toolParams.getIndexParams().toParams());
                    indexParams.setFile(file.getPath());
                    if (!indexStatus.equals(FileIndex.IndexStatus.NONE)) {
                        if (toolParams.isIgnoreFailed()) {
                            logger.info("Skip file '{}' in status {}. ", file.getId(), indexStatus);
                            continue;
                        }
                        if (toolParams.isResumeFailed()) {
                            indexParams.setResume(true);
                        }
                    }
                    String jobId = buildJobId(file);
                    Job job = catalogManager.getJobManager().submit(getStudy(), VariantIndexOperationTool.ID, Enums.Priority.MEDIUM,
                            indexParams.toParams(new ObjectMap(ParamConstants.STUDY_PARAM, study)), jobId, "Job generated by " + getId(),
                            Collections.emptyList(), Collections.emptyList(), getToken()).first();
                    logger.info("Create variant-index job '{}' for file '{}'{}",
                            job.getId(),
                            file.getId(),
                            indexParams.isResume() ? (" with resume=true from indexStatus=" + indexStatus) : "");
                    submittedJobs++;
                } else {
                    logger.info("Skip file '{}' as it's already being loaded by job {}",
                            file.getId(),
                            jobsFromFile.getResults().stream().map(Job::getId).collect(Collectors.toList()));
                }
            }
        }
        addAttribute("submittedJobs", submittedJobs);
        addAttribute("scannedFiles", scannedFiles);
        if (scannedFiles == 0) {
            addWarning("No files found. Nothing to do");
        }
        logger.info("Submitted {} jobs", submittedJobs);
    }

    private String buildJobId(File file) {
        String fileName = file.getName();
        if (fileName.length() > 33) {
            fileName = fileName.substring(0, 30) + "...";
        }
        return "index_" + fileName + "_" + TimeUtils.getTime();
    }

    private String getIndexStatus(File file) {
        String indexStatus;
        if (file.getInternal() == null
                || file.getInternal().getIndex() == null
                || file.getInternal().getIndex().getStatus() == null
                || file.getInternal().getIndex().getStatus().getName() == null) {
            indexStatus = FileIndex.IndexStatus.NONE;
        } else {
            indexStatus = file.getInternal().getIndex().getStatus().getName();
        }
        return indexStatus;
    }
}
