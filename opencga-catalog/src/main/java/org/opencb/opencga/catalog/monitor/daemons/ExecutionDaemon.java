/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.monitor.daemons;

import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.catalog.models.update.JobUpdateParams;
import org.opencb.opencga.core.analysis.result.AnalysisResult;
import org.opencb.opencga.core.analysis.result.AnalysisResultManager;
import org.opencb.opencga.core.analysis.result.Status;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.results.OpenCGAResult;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by imedina on 16/06/16.
 */
public class ExecutionDaemon extends MonitorParentDaemon {

    public static final String OUTDIR_PARAM = "outdir";
    private String internalCli;
    private JobManager jobManager;
    private FileManager fileManager;
    private CatalogIOManager catalogIOManager;

    private Path defaultJobDir;

    // Maximum number of jobs of each type (Pending, queued, running) that will be handled on each iteration.
    // Example: If there are 100 pending jobs, 15 queued, 70 running.
    // On first iteration, it will queue 50 out of the 100 pending jobs. It will check up to 50 queue-running changes out of the 65
    // (15 + 50 from pending), and it will check up to 50 finished jobs from the running ones.
    // On second iteration, it will queue the remaining 50 pending jobs, and so on...
    private static final int NUM_JOBS_HANDLED = 50;
    private final Query pendingJobsQuery;
    private final Query queuedJobsQuery;
    private final Query runningJobsQuery;
    private final QueryOptions queryOptions;

    public ExecutionDaemon(int interval, String token, CatalogManager catalogManager, String appHome)
            throws CatalogDBException, CatalogIOException {
        super(interval, token, catalogManager);

        this.jobManager = catalogManager.getJobManager();
        this.fileManager = catalogManager.getFileManager();
        this.catalogIOManager = catalogManager.getCatalogIOManagerFactory().get("file");
        this.internalCli = appHome + "/bin/opencga-internal.sh";

        this.defaultJobDir = Paths.get(catalogManager.getConfiguration().getJobDir());

        pendingJobsQuery = new Query(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.PENDING);
        queuedJobsQuery = new Query(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.QUEUED);
        runningJobsQuery = new Query(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Enums.ExecutionStatus.RUNNING);
        // Sort jobs by priority and creation date
        queryOptions = new QueryOptions()
                .append(QueryOptions.SORT, Arrays.asList(JobDBAdaptor.QueryParams.PRIORITY.key(),
                        JobDBAdaptor.QueryParams.CREATION_DATE.key()))
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);
    }

    @Override
    public void run() {

        while (!exit) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }

            try {
                checkJobs();
            } catch (Exception e) {
                logger.error("Catch exception " + e.getMessage(), e);
            }
        }
    }

    protected void checkJobs() {
        long pendingJobs = -1;
        long queuedJobs = -1;
        long runningJobs = -1;
        try {
            pendingJobs = jobManager.count(pendingJobsQuery, token).getNumMatches();
            queuedJobs = jobManager.count(queuedJobsQuery, token).getNumMatches();
            runningJobs = jobManager.count(runningJobsQuery, token).getNumMatches();
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
        logger.info("----- EXECUTION DAEMON  ----- pending={}, queued={}, running={}", pendingJobs, queuedJobs, runningJobs);

            /*
            PENDING JOBS
             */
        checkPendingJobs();

            /*
            QUEUED JOBS
             */
        checkQueuedJobs();

            /*
            RUNNING JOBS
             */
        checkRunningJobs();
    }

    protected void checkRunningJobs() {
        int handledRunningJobs = 0;
        try (DBIterator<Job> iterator = jobManager.iterator(runningJobsQuery, queryOptions, token)) {
            while (handledRunningJobs < NUM_JOBS_HANDLED && iterator.hasNext()) {
                Job job = iterator.next();
                handledRunningJobs += checkRunningJob(job);
            }
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    protected int checkRunningJob(Job job) {
        Enums.ExecutionStatus jobStatus = getCurrentStatus(job);

        if (Enums.ExecutionStatus.RUNNING.equals(jobStatus.getName())) {
            AnalysisResult result = readAnalysisResult(job);
            if (result != null) {
                // Update the result of the job
                JobUpdateParams updateParams = new JobUpdateParams().setResult(result);
                String study = String.valueOf(job.getAttributes().get(Job.OPENCGA_STUDY));
                try {
                    jobManager.update(study, job.getId(), updateParams, QueryOptions.empty(), token);
                } catch (CatalogException e) {
                    logger.error("{} - Could not update result information: {}", job.getId(), e.getMessage(), e);
                    return 0;
                }
            }

            return 1;
        } else {
            // Register job results
            return processFinishedJob(job);
        }
    }

    protected void checkQueuedJobs() {
        int handledQueuedJobs = 0;
        try (DBIterator<Job> iterator = jobManager.iterator(queuedJobsQuery, queryOptions, token)) {
            while (handledQueuedJobs < NUM_JOBS_HANDLED && iterator.hasNext()) {
                Job job = iterator.next();
                handledQueuedJobs += checkQueuedJob(job);
            }
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    /**
     * Check if the job is still queued or it has changed to running or error.
     *
     * @param job Job object.
     * @return 1 if the job has changed the status, 0 otherwise.
     */
    protected int checkQueuedJob(Job job) {
        Enums.ExecutionStatus status = getCurrentStatus(job);

        if (Enums.ExecutionStatus.QUEUED.equals(status.getName())) {
            // Job is still queued
            return 0;
        }

        if (Enums.ExecutionStatus.RUNNING.equals(status.getName())) {
            logger.info("Updating job {} from {} to {}", job.getId(), Enums.ExecutionStatus.QUEUED, Enums.ExecutionStatus.RUNNING);
            return setStatus(job, new Enums.ExecutionStatus(Enums.ExecutionStatus.RUNNING));
        }

        // Job has finished the execution, so we need to register the job results
        return processFinishedJob(job);
    }

    protected void checkPendingJobs() {
        int handledPendingJobs = 0;
        try (DBIterator<Job> iterator = jobManager.iterator(pendingJobsQuery, queryOptions, token)) {
            while (handledPendingJobs < NUM_JOBS_HANDLED && iterator.hasNext()) {
                Job job = iterator.next();
                handledPendingJobs += checkPendingJob(job);
            }
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    /**
     * Check everything is correct and queues the job.
     *
     * @param job Job object.
     * @return 1 if the job has changed the status, 0 otherwise.
     */
    protected int checkPendingJob(Job job) {
        String study = String.valueOf(job.getAttributes().get(Job.OPENCGA_STUDY));
        if (StringUtils.isEmpty(study)) {
            return abortJob(job, "Missing mandatory '" + Job.OPENCGA_STUDY + "' field");
        }

        String command = String.valueOf(job.getAttributes().get(Job.OPENCGA_COMMAND));
        if (StringUtils.isEmpty(command)) {
            return abortJob(job, "Missing mandatory '" + Job.OPENCGA_COMMAND + "' field");
        }

        String subcommand = String.valueOf(job.getAttributes().get(Job.OPENCGA_SUBCOMMAND));
        if (StringUtils.isEmpty(subcommand)) {
            return abortJob(job, "Missing mandatory '" + Job.OPENCGA_SUBCOMMAND + "' field");
        }

        if (!canBeQueued(job)) {
            return 0;
        }

        String userToken;
        try {
            userToken = catalogManager.getUserManager().getSystemTokenForUser(job.getUserId(), token);
        } catch (CatalogException e) {
            return abortJob(job, "Internal error. Could not obtain token for user '" + job.getUserId() + "'");
        }

        JobUpdateParams updateParams = new JobUpdateParams();

        Map<String, Object> params = job.getParams();
        String outDirPathParam = (String) params.get(OUTDIR_PARAM);
        if (!StringUtils.isEmpty(outDirPathParam)) {
            try {
                // Any path the user has requested
                updateParams.setOutDir(getValidInternalOutDir(study, job, outDirPathParam, userToken));
            } catch (CatalogException e) {
                logger.error("Cannot create output directory. {}", e.getMessage(), e);
                return abortJob(job, "Cannot create output directory. " + e.getMessage());
            }
        } else {
            try {
                // JOBS/user/job_id/
                updateParams.setOutDir(getValidDefaultOutDir(study, job, userToken));
            } catch (CatalogException e) {
                logger.error("Cannot create output directory. {}", e.getMessage(), e);
                return abortJob(job, "Cannot create output directory. " + e.getMessage());
            }
        }

        Path outDirPath = Paths.get(updateParams.getOutDir().getUri());
        params.put(OUTDIR_PARAM, outDirPath.toAbsolutePath().toString());

        // Define where the stdout and stderr will be stored
        Path stderr = outDirPath.resolve(getErrorLogFileName(job));
        Path stdout = outDirPath.resolve(getLogFileName(job));

        // Create cli
        String commandLine = buildCli(internalCli, command, subcommand, params);
        String authenticatedCommandLine = commandLine + " --token " + userToken;
        String shadedCommandLine = commandLine + " --token xxxxxxxxxxxxxxxxxxxxx";

        updateParams.setCommandLine(shadedCommandLine);

        logger.info("Updating job {} from {} to {}", job.getId(), Enums.ExecutionStatus.PENDING, Enums.ExecutionStatus.QUEUED);
        updateParams.setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.QUEUED));
        try {
            jobManager.update(study, job.getId(), updateParams, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            logger.error("Could not update job {}. {}", job.getId(), e.getMessage(), e);
            return 0;
        }

        try {
            batchExecutor.execute(job.getId(), authenticatedCommandLine, stdout, stderr);
        } catch (Exception e) {
            logger.error("Error executing job {}.", job.getId(), e);
            return abortJob(job, "Error executing job. " + e.getMessage());
        }
        return 1;
    }

    private File getValidInternalOutDir(String study, Job job, String outDirPath, String userToken) throws CatalogException {
        // TODO: Remove this line when we stop passing the outdir as a query param in the URL
        outDirPath = outDirPath.replace(":", "/");
        if (!outDirPath.endsWith("/")) {
            outDirPath += "/";
        }
        File outDir;
        try {
            outDir = fileManager.get(study, outDirPath, FileManager.INCLUDE_FILE_URI_PATH, token).first();
        } catch (CatalogException e) {
            // Directory not found. Will try to create using user's token
            boolean parents = (boolean) job.getAttributes().getOrDefault(Job.OPENCGA_PARENTS, false);
            try {
                outDir = fileManager.createFolder(study, outDirPath, new File.FileStatus(), parents, "", FileManager.INCLUDE_FILE_URI_PATH,
                        userToken).first();
                CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(outDir.getUri());
                ioManager.createDirectory(outDir.getUri(), true);
            } catch (CatalogException e1) {
                throw new CatalogException("Cannot create output directory. " + e1.getMessage(), e1.getCause());
            }
        }

        // Ensure the directory is empty
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(outDir.getUri());
        if (!ioManager.isDirectory(outDir.getUri())) {
            throw new CatalogException(OUTDIR_PARAM + " seems not to be a directory");
        }
        if (!ioManager.listFiles(outDir.getUri()).isEmpty()) {
            throw new CatalogException(OUTDIR_PARAM + " " + outDirPath + " is not an empty directory");
        }

        return outDir;
    }

    private File getValidDefaultOutDir(String studyStr, Job job, String userToken) throws CatalogException {
        OpenCGAResult<File> fileOpenCGAResult;
        try {
            fileOpenCGAResult = fileManager.get(studyStr, "JOBS/", FileManager.INCLUDE_FILE_URI_PATH, token);
        } catch (CatalogException e) {
            logger.info("JOBS/ directory does not exist, registering for the first time");

            // Create main JOBS directory for the study
            Study study = catalogManager.getStudyManager().resolveId(studyStr, "admin");
            long projectUid = catalogManager.getProjectManager().get(study.getFqn().split(":")[0], new QueryOptions(QueryOptions.INCLUDE,
                    ProjectDBAdaptor.QueryParams.UID.key()), token).first().getUid();

            URI uri = Paths.get(catalogManager.getConfiguration().getJobDir())
                    .resolve(study.getFqn().split("@")[0]) // user
                    .resolve(Long.toString(projectUid))
                    .resolve(Long.toString(study.getUid()))
                    .resolve("JOBS")
                    .toUri();

            // Create the directory in the file system
            catalogIOManager.createDirectory(uri, true);
            // And link it to OpenCGA
            fileOpenCGAResult = fileManager.link(studyStr, uri, "/", new ObjectMap("parents", true), token);
        }

        // Check we can write in the folder
        catalogIOManager.checkWritableUri(fileOpenCGAResult.first().getUri());

        // Check if the default jobOutDirPath of the user already exists
        OpenCGAResult<File> result;
        try {
            result = fileManager.get(studyStr, "JOBS/" + job.getUserId() + "/", FileManager.INCLUDE_FILE_URI_PATH, userToken);
        } catch (CatalogException e) {
            // We first need to create the main directory that will contain all the jobs of the user
            result = fileManager.createFolder(studyStr, "JOBS/" + job.getUserId() + "/", new File.FileStatus(), false,
                    "Directory containing the jobs of " + job.getUserId(), FileManager.INCLUDE_FILE_URI_PATH, token);

            // Add permissions to do anything under that path to the user launching the job
            String allFilePermissions = EnumSet.allOf(FileAclEntry.FilePermissions.class)
                    .stream()
                    .map(FileAclEntry.FilePermissions::toString)
                    .collect(Collectors.joining(","));
            fileManager.updateAcl(studyStr, Collections.singletonList("JOBS/" + job.getUserId() + "/"), job.getUserId(),
                    new File.FileAclParams(allFilePermissions, AclParams.Action.SET, null), token);
            // Remove permissions to any other user that is not the one launching the job
            fileManager.updateAcl(studyStr, Collections.singletonList("JOBS/" + job.getUserId() + "/"), FileAclEntry.USER_OTHERS_ID,
                    new File.FileAclParams("", AclParams.Action.SET, null), token);
        }

        // Now we create a new directory where the job will be actually executed
        File userFolder = result.first();

        File outDirFile = fileManager.createFolder(studyStr, userFolder.getPath() + job.getId(), new File.FileStatus(), false,
                "Directory containing the results of the execution of job " + job.getId(), FileManager.INCLUDE_FILE_URI_PATH, token)
                .first();

        // Create the physical directories in disk
        try {
            catalogIOManager.createDirectory(outDirFile.getUri(), true);
        } catch (CatalogIOException e) {
            throw new CatalogException("Cannot create job directories '" + outDirFile.getUri() + "' for path '" + outDirFile.getPath()
                    + "'");
        }

        return outDirFile;
    }

    public static String buildCli(String internalCli, String command, String subcommand, Map<String, Object> params) {
        StringBuilder cliBuilder = new StringBuilder()
                .append(internalCli).append(" ")
                .append(command).append(" ")
                .append(subcommand);
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (entry.getValue() instanceof Map) {
                Map<String, String> dynamicParams = (Map<String, String>) entry.getValue();
                for (Map.Entry<String, String> dynamicEntry : dynamicParams.entrySet()) {
                    cliBuilder
                            .append(" ").append("-D").append(dynamicEntry.getKey())
                            .append("=").append(dynamicEntry.getValue());
                }
            } else {
                cliBuilder
                        .append(" --").append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, entry.getKey()))
                        .append(" ").append(entry.getValue());
            }
        }
        return cliBuilder.toString();
    }

    private boolean canBeQueued(Job job) {
        String command = String.valueOf(job.getAttributes().get(Job.OPENCGA_COMMAND));
        String subcommand = String.valueOf(job.getAttributes().get(Job.OPENCGA_SUBCOMMAND));

        if ("variant".equals(command) && "index".equals(subcommand)) {
            int maxIndexJobs = catalogManager.getConfiguration().getAnalysis().getIndex().getVariant().getMaxConcurrentJobs();
            logger.info("TODO: Check maximum number of slots for variant index");
            // TODO: Check maximum number of slots for variant index
            int currentVariantIndexJobs = 0;
            if (currentVariantIndexJobs > maxIndexJobs) {
                logger.info("{} index jobs running or in queue already. "
                        + "Current limit is {}. "
                        + "Skipping new index job '{}' temporary", currentVariantIndexJobs, maxIndexJobs, job.getId());
                return false;
            }
        }

        return true;
    }

    private int abortJob(Job job, String description) {
        logger.info("Aborting job: {} - Reason: '{}'", job.getId(), description);
        return setStatus(job, new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED, description));
    }

    private int setStatus(Job job, Enums.ExecutionStatus status) {
        JobUpdateParams updateParams = new JobUpdateParams().setStatus(status);

        String study = String.valueOf(job.getAttributes().get(Job.OPENCGA_STUDY));
        if (StringUtils.isEmpty(study)) {
            try {
                study = jobManager.getStudy(job, token).getFqn();
            } catch (CatalogException e) {
                logger.error("Unexpected error. Unknown study of job '{}'. {}", job.getId(), e.getMessage(), e);
                return 0;
            }
        }

        try {
            jobManager.update(study, job.getId(), updateParams, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            logger.error("Unexpected error. Cannot update job '{}' to status '{}'. {}", job.getId(), updateParams.getStatus().getName(),
                    e.getMessage(), e);
            return 0;
        }

        return 1;
    }

    private Enums.ExecutionStatus getCurrentStatus(Job job) {

        Path resultJson = getAnalysisResultPath(job);

        // Check if analysis result file is there
        if (resultJson != null && Files.exists(resultJson)) {
            AnalysisResult analysisResult = readAnalysisResult(resultJson);
            if (analysisResult != null) {
                return new Enums.ExecutionStatus(analysisResult.getStatus().getName().name());
            } else {
                if (Files.exists(resultJson)) {
                    logger.warn("File '" + resultJson + "' seems corrupted.");
                } else {
                    logger.warn("Could not find file '" + resultJson + "'.");
                }
            }
        }

        String status = batchExecutor.getStatus(job.getId());
        if (!StringUtils.isEmpty(status) && !status.equals(Enums.ExecutionStatus.UNKNOWN)) {
            return new Enums.ExecutionStatus(status);
        } else {
            Path tmpOutdirPath = Paths.get(job.getOutDir().getUri());
            // Check if the error file is present
            Path errorLog = tmpOutdirPath.resolve(getErrorLogFileName(job));

            if (Files.exists(errorLog)) {
                // FIXME: This may not be true. There is a delay between job starts (i.e. error log appears) and
                //  the analysis result creation

                // There must be some command line error. The job started running but did not finish well, otherwise we would find the
                // analysis-result.yml file
                return new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR, "Command line error");
            } else {
                return new Enums.ExecutionStatus(Enums.ExecutionStatus.QUEUED);
            }
        }

    }

    private Path getAnalysisResultPath(Job job) {
        Path resultJson = null;
        try (Stream<Path> stream = Files.list(Paths.get(job.getOutDir().getUri()))) {
            resultJson = stream
                    .filter(path -> {
                        String str = path.toString();
                        return str.endsWith(AnalysisResultManager.FILE_EXTENSION)
                                && !str.endsWith(AnalysisResultManager.SWAP_FILE_EXTENSION);
                    })
                    .findFirst()
                    .orElse(null);
        } catch (IOException e) {
            logger.warn("Could not find AnalysisResult file", e);
        }
        return resultJson;
    }

    private AnalysisResult readAnalysisResult(Job job) {
        Path resultJson = getAnalysisResultPath(job);
        if (resultJson != null) {
            return readAnalysisResult(resultJson);
        }
        return null;
    }

    private AnalysisResult readAnalysisResult(Path file) {
        if (file == null) {
            return null;
        }
        int attempts = 0;
        int maxAttempts = 3;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                try (InputStream is = new BufferedInputStream(new FileInputStream(file.toFile()))) {
                    return JacksonUtils.getDefaultObjectMapper().readValue(is, AnalysisResult.class);
                }
            } catch (IOException e) {
                if (attempts == maxAttempts) {
                    logger.error("Could not load AnalysisResult file: " + file.toAbsolutePath(), e);
                } else {
                    logger.warn("Could not load AnalysisResult file: " + file.toAbsolutePath()
                            + ". Retry " + attempts + "/" + maxAttempts
                            + ". " + e.getMessage()
                    );
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException interruption) {
                        // Ignore interruption
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        return null;
    }

    private int processFinishedJob(Job job) {
        logger.info("{} - Processing finished job...", job.getId());

        Path outDirUri = Paths.get(job.getOutDir().getUri());
        Path analysisResultPath = getAnalysisResultPath(job);

        String study = String.valueOf(job.getAttributes().get(Job.OPENCGA_STUDY));

        logger.info("{} - Registering job results from '{}'", job.getId(), outDirUri);

        AnalysisResult analysisResult;
        if (analysisResultPath != null) {
            analysisResult = readAnalysisResult(analysisResultPath);
            if (analysisResult != null) {
                JobUpdateParams updateParams = new JobUpdateParams().setResult(analysisResult);
                try {
                    jobManager.update(study, job.getId(), updateParams, QueryOptions.empty(), token);
                } catch (CatalogException e) {
                    logger.error("{} - Catastrophic error. Could not update job information with final result {}: {}", job.getId(),
                            updateParams.toString(), e.getMessage(), e);
                    return 0;
                }
            }
        } else {
            analysisResult = null;
        }

        List<File> registeredFiles;
        try {
            Predicate<URI> uriPredicate = uri -> !uri.getPath().endsWith(AnalysisResultManager.FILE_EXTENSION)
                    && !uri.getPath().endsWith(AnalysisResultManager.SWAP_FILE_EXTENSION)
                    && !uri.getPath().contains("/scratch_");
            registeredFiles = fileManager.syncUntrackedFiles(study, job.getOutDir().getPath(), uriPredicate, token).getResults();
        } catch (CatalogException e) {
            logger.error("Could not registered files in Catalog: {}", e.getMessage(), e);
            return 0;
        }

        // Register the job information
        JobUpdateParams updateParams = new JobUpdateParams();

        // Process output and log files
        List<File> outputFiles = new ArrayList<>(registeredFiles.size());
        String logFileName = getLogFileName(job);
        String errorLogFileName = getErrorLogFileName(job);
        for (File registeredFile : registeredFiles) {
            if (registeredFile.getName().equals(logFileName)) {
                updateParams.setLog(registeredFile);
            } else if (registeredFile.getName().equals(errorLogFileName)) {
                updateParams.setErrorLog(registeredFile);
            } else {
                outputFiles.add(registeredFile);
            }
        }
        updateParams.setOutput(outputFiles);


        // Check status of analysis result or if there are files that could not be moved to outdir to decide the final result
        if (analysisResult == null) {
            updateParams.setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR, "Job could not finish successfully. "
                    + "Missing analysis result"));
        } else if (analysisResult.getStatus().getName().equals(Status.Type.ERROR)) {
            updateParams.setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR, "Job could not finish successfully"));
        } else {
            updateParams.setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE));
        }

        logger.info("{} - Updating job information", job.getId());
        // We update the job information
        try {
            jobManager.update(study, job.getId(), updateParams, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            logger.error("{} - Catastrophic error. Could not update job information with final result {}: {}", job.getId(),
                    updateParams.toString(), e.getMessage(), e);
            return 0;
        }

        return 1;
    }

    private String getErrorLogFileName(Job job) {
        return job.getId() + ".err";
    }

    private String getLogFileName(Job job) {
        return job.getId() + ".log";
    }

}
