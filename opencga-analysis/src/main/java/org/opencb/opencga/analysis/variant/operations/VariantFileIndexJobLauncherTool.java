package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.lang3.StringUtils;
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
import org.opencb.opencga.core.models.file.FileInternal;
import org.opencb.opencga.core.models.file.VariantIndexStatus;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.variant.VariantFileIndexJobLauncherParams;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
                .append(FORMAT.key(), Arrays.asList(File.Format.VCF, File.Format.BCF, File.Format.GVCF))
                .append(INTERNAL_VARIANT_INDEX_STATUS_ID.key(), "!" + VariantIndexStatus.READY);
        filesQuery.putIfNotEmpty(NAME.key(), toolParams.getName());
        filesQuery.putIfNotEmpty(DIRECTORY.key(), toolParams.getDirectory());
        QueryOptions filesInclude = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                FileDBAdaptor.QueryParams.ID.key(),
                FileDBAdaptor.QueryParams.NAME.key(),
                FileDBAdaptor.QueryParams.PATH.key(),
                FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX.key()));

        int submittedJobs = 0;
        int filesPerJob = 1;
        int maxJobs = Integer.MAX_VALUE;
        if (toolParams.getMaxJobs() > 0) {
            maxJobs = toolParams.getMaxJobs();
            logger.info("Creating up to " + maxJobs + " jobs");
        }

        int scannedFiles = 0;
        List<File> files = new ArrayList<>(200);
        try (DBIterator<File> dbIterator = getCatalogManager().getFileManager()
                .iterator(getStudy(), filesQuery, filesInclude, getToken())) {
            while (dbIterator.hasNext()) {
                files.add(dbIterator.next());
            }
        }
        Collections.shuffle(files);

        for (File file : files) {
            if (submittedJobs == maxJobs) {
                logger.info("Submitted {} jobs. Max number of submitted jobs reached. Stop!", submittedJobs);
                break;
            }
            scannedFiles++;
            String indexStatus = getVariantIndexStatus(file);
            OpenCGAResult<Job> jobsFromFile = getCatalogManager()
                    .getJobManager()
                    .search(getStudy(),
                            new Query()
                                    .append(JobDBAdaptor.QueryParams.INPUT.key(), file.getId())
                                    .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), VariantIndexOperationTool.ID)
                                    .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Arrays.asList(
                                            Enums.ExecutionStatus.RUNNING,
                                            Enums.ExecutionStatus.QUEUED,
                                            Enums.ExecutionStatus.ERROR,
                                            Enums.ExecutionStatus.PENDING
                                    )),
                            new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                    JobDBAdaptor.QueryParams.ID.key(),
                                    JobDBAdaptor.QueryParams.INTERNAL_STATUS.key())),
                            getToken());

            long errorJobs = jobsFromFile.getResults().stream()
                    .filter(j -> j.getInternal().getStatus().getId().equals(Enums.ExecutionStatus.ERROR)).count();
            long runningJobs = jobsFromFile.getNumResults() - errorJobs;
            if (runningJobs == 0) {
                // The file is not indexed and it's not in any pending job.
                VariantIndexParams indexParams = new VariantIndexParams();
                indexParams.updateParams(toolParams.getIndexParams().toParams());
                indexParams.setFile(file.getPath());
                if (!indexStatus.equals(VariantIndexStatus.NONE) || errorJobs != 0) {
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
                submittedJobs++;
                logger.info("[{}] Create variant-index job '{}' for file '{}'{}",
                        submittedJobs,
                        job.getId(),
                        file.getId(),
                        indexParams.isResume() ? (" with resume=true from indexStatus=" + indexStatus) : "");
            } else {
                logger.info("Skip file '{}' as it's already being loaded by job {}",
                        file.getId(),
                        jobsFromFile.getResults().stream().map(Job::getId).collect(Collectors.toList()));
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
        fileName = StringUtils.remove(fileName, '%');
        if (fileName.length() > 30) {
            fileName = fileName.substring(0, 30);
        }
        return "index_" + fileName + "_" + TimeUtils.getTimeMillis();
    }

    public static String getVariantIndexStatus(File file) {
        return FileInternal.getVariantIndexStatusId(file.getInternal());
    }

}
