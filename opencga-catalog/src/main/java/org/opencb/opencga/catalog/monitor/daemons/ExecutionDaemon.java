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
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.catalog.models.update.JobUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.analysis.result.AnalysisResult;
import org.opencb.opencga.core.analysis.result.AnalysisResultManager;
import org.opencb.opencga.core.analysis.result.Status;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.results.OpenCGAResult;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by imedina on 16/06/16.
 */
public class ExecutionDaemon extends MonitorParentDaemon {

    private String internalCli;
    private JobManager jobManager;
    private FileManager fileManager;
    private CatalogIOManager catalogIOManager;
    private static final String ANALYSIS_RESULT_FILE = "analysis-result.yml";

    // Maximum number of jobs of each type (Pending, queued, running) that will be handled on each iteration.
    // Example: If there are 100 pending jobs, 15 queued, 70 running.
    // On first iteration, it will queue 50 out of the 100 pending jobs. It will check up to 50 queue-running changes out of the 65
    // (15 + 50 from pending), and it will check up to 50 finished jobs from the running ones.
    // On second iteration, it will queue the remaining 50 pending jobs, and so on...
    private static final int NUM_JOBS_HANDLED = 50;

    public ExecutionDaemon(int interval, String token, CatalogManager catalogManager, String appHome)
            throws CatalogDBException, CatalogIOException {
        super(interval, token, catalogManager);
        this.jobManager = catalogManager.getJobManager();
        this.fileManager = catalogManager.getFileManager();
        this.catalogIOManager = catalogManager.getCatalogIOManagerFactory().get("file");
        this.internalCli = appHome + "/bin/opencga-internal.sh";
    }

    @Override
    public void run() {

        Query pendingJobsQuery = new Query(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.PENDING);
        Query queuedJobsQuery = new Query(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.QUEUED);
        Query runningJobsQuery = new Query(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.RUNNING);
        // Sort jobs by priority and creation date
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.SORT, Arrays.asList(JobDBAdaptor.QueryParams.PRIORITY.key(),
                        JobDBAdaptor.QueryParams.CREATION_DATE.key()))
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);

        while (!exit) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- EXECUTION DAEMON {} -----", TimeUtils.getTimeMillis());

            /*
            PENDING JOBS
             */
            try {
                long count = jobManager.count(pendingJobsQuery, token).getNumMatches();
                logger.debug("Checking pending jobs. {} jobs found", count);
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            int handledPendingJobs = 0;
            try (DBIterator<Job> iterator = jobManager.iterator(pendingJobsQuery, queryOptions, token)) {
                while (handledPendingJobs < NUM_JOBS_HANDLED && iterator.hasNext()) {
                    handledPendingJobs += checkPendingJob(iterator.next());
                }
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            /*
            QUEUED JOBS
             */
            try {
                long count = jobManager.count(queuedJobsQuery, token).getNumMatches();
                logger.debug("Checking queued jobs. {} jobs found", count);
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            int handledQueuedJobs = 0;
            try (DBIterator<Job> iterator = jobManager.iterator(queuedJobsQuery, queryOptions, token)) {
                while (handledQueuedJobs < NUM_JOBS_HANDLED && iterator.hasNext()) {
                    handledQueuedJobs += checkQueuedJob(iterator.next());
                }
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            /*
            RUNNING JOBS
             */
            try {
                long count = jobManager.count(runningJobsQuery, token).getNumMatches();
                logger.debug("Checking running jobs. {} running jobs found", count);
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

            int handledRunningJobs = 0;
            try (DBIterator<Job> iterator = jobManager.iterator(runningJobsQuery, queryOptions, token)) {
                while (handledRunningJobs < NUM_JOBS_HANDLED && iterator.hasNext()) {
                    handledRunningJobs += checkRunningJob(iterator.next());
                }
            } catch (CatalogException e) {
                logger.error("{}", e.getMessage(), e);
            }

        }
    }

    private int checkRunningJob(Job job) {
        Job.JobStatus jobStatus = getCurrentStatus(job);

        if (Job.JobStatus.RUNNING.equals(jobStatus.getName())) {
            String analysisId = String.valueOf(job.getAttributes().get(Job.OPENCGA_COMMAND));
            AnalysisResult result = readAnalysisResult(analysisId, Paths.get(job.getTmpDir().getUri()));
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

    /**
     * Check if the job is still queued or it has changed to running or error.
     *
     * @param job Job object.
     * @return 1 if the job has changed the status, 0 otherwise.
     */
    private int checkQueuedJob(Job job) {
        Job.JobStatus status = getCurrentStatus(job);

        if (Job.JobStatus.QUEUED.equals(status.getName())) {
            // Job is still queued
            return 0;
        }

        if (Job.JobStatus.RUNNING.equals(status.getName())) {
            logger.info("Updating job {} from {} to {}", job.getId(), Job.JobStatus.QUEUED, Job.JobStatus.RUNNING);
            return setStatus(job, new Job.JobStatus(Job.JobStatus.RUNNING));
        }

        // Job has finished the execution, so we need to register the job results
        return processFinishedJob(job);
    }

    /**
     * Check everything is correct and queues the job.
     *
     * @param job Job object.
     * @return 1 if the job has changed the status, 0 otherwise.
     */
    private int checkPendingJob(Job job) {
        String study = String.valueOf(job.getAttributes().get(Job.OPENCGA_STUDY));
        if (study.isEmpty()) {
            return abortJob(job, "Missing mandatory '" + Job.OPENCGA_STUDY + "' field");
        }

        String command = String.valueOf(job.getAttributes().get(Job.OPENCGA_COMMAND));
        if (command.isEmpty()) {
            return abortJob(job, "Missing mandatory '" + Job.OPENCGA_COMMAND + "' field");
        }

        String subcommand = String.valueOf(job.getAttributes().get(Job.OPENCGA_SUBCOMMAND));
        if (subcommand.isEmpty()) {
            return abortJob(job, "Missing mandatory '" + Job.OPENCGA_SUBCOMMAND + "' field");
        }

        String outDirPath = String.valueOf(job.getParams().get("outdir"));
        if (outDirPath.isEmpty()) {
            return abortJob(job, "Missing mandatory output directory");
        }
        if (!outDirPath.endsWith("/")) {
            return abortJob(job, "Invalid output directory. Valid directories should end in /");
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

        // TODO: Remove this line when we stop passing the outdir as a query param in the URL
        outDirPath = outDirPath.replace(":", "/");
        try {
            OpenCGAResult<File> fileOpenCGAResult = fileManager.get(study, outDirPath,
                    FileManager.INCLUDE_FILE_URI_PATH, token);
            // Directory exists
            updateParams.setOutDir(fileOpenCGAResult.first());
        } catch (CatalogException e) {
            // Directory not found. Will try to create using user's token
            boolean parents = (boolean) job.getAttributes().getOrDefault(Job.OPENCGA_PARENTS, false);
            try {
                File folder = fileManager.createFolder(study, outDirPath, new File.FileStatus(), parents, "",
                        FileManager.INCLUDE_FILE_URI_PATH, userToken).first();
                updateParams.setOutDir(folder);
            } catch (CatalogException e1) {
                // Directory could not be created
                logger.error("Cannot create output directory. {}", e1.getMessage(), e1);
                return abortJob(job, "Cannot create output directory. " + e1.getMessage());
            }
        }

        // Create temporal directory
        try {
            String tmpDir = updateParams.getOutDir().getPath() + "job_" + job.getId() + "_temp/";
            File folder = fileManager.createFolder(study, tmpDir, new File.FileStatus(), false, "",
                    FileManager.INCLUDE_FILE_URI_PATH, userToken).first();
            updateParams.setTmpDir(folder);
        } catch (CatalogException e) {
            // Directory could not be created
            logger.error("Cannot create temporal directory. {}", e.getMessage(), e);
            return abortJob(job, "Cannot create temporal directory. " + e.getMessage());
        }

        Path temporalPath = Paths.get(updateParams.getTmpDir().getUri());

        // Define where the stdout and stderr will be stored
        Path stderr = temporalPath.resolve(job.getId() + ".err");
        Path stdout = temporalPath.resolve(job.getId() + ".log");

        List<File> inputFiles = new ArrayList<>();

        // Create cli
        StringBuilder cliBuilder = new StringBuilder(internalCli).append(" ")
                .append(command).append(" ")
                .append(subcommand);
        for (Map.Entry<String, String> entry : job.getParams().entrySet()) {
            if (entry.getKey().equals("outdir")) {
                cliBuilder.append(" --outdir ").append(temporalPath);
            } else if (entry.getKey().toLowerCase().endsWith("file")) {
                // We assume that every variable ending in 'file' corresponds to input files that need to be accessible in catalog
                File file;
                try {
                    file = fileManager.get(study, entry.getValue(), FileManager.INCLUDE_FILE_URI_PATH, userToken)
                            .first();
                } catch (CatalogException e) {
                    logger.error("Cannot find file '{}' from variable '{}'. {}", entry.getValue(), entry.getKey(), e.getMessage(), e);
                    return abortJob(job, "Cannot find file '" + entry.getValue() + "' from variable '" + entry.getKey() + "'. "
                            + e.getMessage());
                }
                inputFiles.add(file);

                // And we change the reference for the actual uri
                cliBuilder
                        .append(" --").append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, entry.getKey()))
                        .append(" ").append(file.getUri().getPath());
            } if (entry.getKey().startsWith("-D")) {
                cliBuilder
                        .append(" ").append(entry.getKey())
                        .append("=").append(entry.getValue());
            } else {
                cliBuilder
                        .append(" --").append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, entry.getKey()))
                        .append(" ").append(entry.getValue());
            }
        }
        updateParams.setCommandLine(cliBuilder.toString());
        updateParams.setInput(inputFiles);

        logger.info("Updating job {} from {} to {}", job.getId(), Job.JobStatus.PENDING, Job.JobStatus.QUEUED);
        updateParams.setStatus(new Job.JobStatus(Job.JobStatus.QUEUED));
        try {
            jobManager.update(study, job.getId(), updateParams, QueryOptions.empty(), token);
            executeJob(job.getId(), updateParams.getCommandLine(), stdout, stderr, userToken);
        } catch (CatalogException e) {
            logger.error("Could not update job {}. {}", job.getId(), e.getMessage(), e);
            return 0;
        }

        return 1;
    }

    private boolean canBeQueued(Job job) {
        String command = String.valueOf(job.getAttributes().get(Job.OPENCGA_COMMAND));
        String subcommand = String.valueOf(job.getAttributes().get(Job.OPENCGA_SUBCOMMAND));

        if ("variant".equals(command) && "index".equals(subcommand)) {
            // TODO: Check maximum number of slots for variant index
            logger.info("{} index jobs running or in queue already. Skipping new index job '{}' temporary", 10, job.getId());
            return false;
        }

        return true;
    }

    private int abortJob(Job job, String description) {
        logger.info("{} - Aborting job...", job.getId());
        return setStatus(job, new Job.JobStatus(Job.JobStatus.ABORTED, description));
    }

    private int setStatus(Job job, Job.JobStatus status) {
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

    private Job.JobStatus getCurrentStatus(Job job) {
        Path tmpOutdirPath = Paths.get(job.getTmpDir().getUri());

        // Check if analysis result file is there
        if (Files.exists(tmpOutdirPath.resolve(ANALYSIS_RESULT_FILE))) {
            String analysisId = String.valueOf(job.getAttributes().get(Job.OPENCGA_COMMAND));
            AnalysisResult analysisResult = readAnalysisResult(analysisId, tmpOutdirPath);
            if (analysisResult != null) {
                return new Job.JobStatus(analysisResult.getStatus().getName().name());
            } else {
                return new Job.JobStatus(Job.JobStatus.ERROR, "File '" + ANALYSIS_RESULT_FILE + "' seems corrupted.");
            }
        } else {
            // Check if the error file is present
            Path errorLog = tmpOutdirPath.resolve(job.getId() + ".err");

            if (Files.exists(errorLog)) {
                // There must be some command line error. The job started running but did not finish well, otherwise we would find the
                // analysis-result.yml file
                return new Job.JobStatus(Job.JobStatus.ERROR, "Command line error");
            } else {
                return new Job.JobStatus(Job.JobStatus.QUEUED);
            }
        }
    }

    private AnalysisResult readAnalysisResult(String analysisId, Path directory) {
        AnalysisResultManager manager;
        int maxAttempts = 0;
        try {
            manager = new AnalysisResultManager(analysisId, directory);
        } catch (AnalysisException e) {
            logger.error("Could not initialise AnalysisResultManager {}", e.getMessage(), e);
            return null;
        }
        while (maxAttempts < 2) {
            try {
                return manager.read();
            } catch (AnalysisException e) {
                logger.warn("Could not load AnalysisResult file. {}", e.getMessage(), e);
                maxAttempts++;
            }
        }

        return null;
    }

    private int processFinishedJob(Job job) {
        logger.info("{} - Processing finished job...", job.getId());

        Path outDirPath = Paths.get(job.getOutDir().getPath());
        Path outDirUri = Paths.get(job.getOutDir().getUri());
        URI tmpOutdirUri = job.getTmpDir().getUri();

        String study = String.valueOf(job.getAttributes().get(Job.OPENCGA_STUDY));

        logger.info("{} - Moving data from temporary folder {} to catalog folder {}", job.getId(), tmpOutdirUri, outDirUri);

        // Because we don't want to lose any data and an error could eventually happen while we are processing the files, we will first
        // read the list of output files from the Job information in case this ever happened and some files were already processed.
        Set<String> outputFileIdSet = new HashSet<>();
        List<File> outputFiles = new ArrayList<>();
        if (job.getOutput() != null && !job.getOutput().isEmpty()) {
            for (File file : job.getOutput()) {
                outputFileIdSet.add(file.getPath());
                outputFiles.add(file);
            }
        }

        // For each file (apart from analysis-result.yml), try to register it in outDirPath
        Iterator<URI> uriIterator;
        try {
            uriIterator = catalogIOManager.listFilesStream(tmpOutdirUri).iterator();
        } catch (CatalogIOException e) {
            logger.error("{} - Could not list files from temporal directory {}: {}", job.getId(), tmpOutdirUri, e.getMessage(), e);
            return 0;
        }

        boolean allFilesMoved = true;

        while (uriIterator.hasNext()) {
            URI fileUri = uriIterator.next();
            java.io.File file = new java.io.File(fileUri);

            if (ANALYSIS_RESULT_FILE.equals(file.getName())) {
                // We will handle this file when everything is moved.
                continue;
            }

            Path finalFilePath = outDirPath.resolve(file.getName());
            Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), finalFilePath);
            File registeredFile = null;

            URI finalFileUri = finalFilePath.toUri();
            // If there is not file registered under that name in the final path and there is no file with the same name in the file system
            // in the final uri

            try {
                if (fileManager.count(study, query, token).getNumMatches() == 0 && !catalogIOManager.exists(finalFileUri)) {
                    logger.info("{} - Moving and registering file to {}", job.getId(), finalFilePath);

                    // We can directly move the file ...
                    catalogIOManager.moveFile(fileUri, finalFileUri);

                    // Register the file in catalog
                    registeredFile = fileManager.register(study, finalFilePath, FileManager.INCLUDE_FILE_URI_PATH, token).first();
                } else {
                    logger.warn("{} - File '{}' already exists in the final path. Registering directly from the temporal directory",
                            job.getId(), file.getName());

                    // The path is in use. Cannot move file to final path so we register it from the temporal directory
                    registeredFile = fileManager.register(study, file.toPath(), FileManager.INCLUDE_FILE_URI_PATH, token).first();

                    // We mark that there has been a problem and at least this file was not possible to be moved to the final directory
                    allFilesMoved = false;
                }
            } catch (CatalogException e) {
                logger.error("{} - Unexpected error while processing the files: {}", job.getId(), e.getMessage(), e);

                if (registeredFile != null) {
                    if (!outputFileIdSet.contains(registeredFile.getPath())) {
                        outputFiles.add(registeredFile);
                        outputFileIdSet.add(registeredFile.getPath());
                    }
                }
                if (!outputFiles.isEmpty()) {
                    // Because some of the files will probably have been moved and deleted, we will store the output files in job so this
                    // information is not lost
                    if (job.getOutput() == null || job.getOutput().size() < outputFiles.size()) {
                        JobUpdateParams updateParams = new JobUpdateParams().setOutput(outputFiles);
                        logger.error("{} - Registering processed output files in job", job.getId());
                        try {
                            jobManager.update(study, job.getId(), updateParams, QueryOptions.empty(), token);
                        } catch (CatalogException e1) {
                            logger.error("{} - Catastrophic error. Could not save processed output files in job: {}", job.getId(),
                                    e1.getMessage(), e1);
                            logger.error("{} - List of job paths that could not be registered is: {}", job.getId(), outputFileIdSet);
                        }
                    }
                }

                return 0;
            }

            // Add registered file to list of output files of Job
            if (!outputFileIdSet.contains(registeredFile.getPath())) {
                outputFiles.add(registeredFile);
                outputFileIdSet.add(registeredFile.getPath());
            }
        }

        // Register the job information
        JobUpdateParams updateParams = new JobUpdateParams();
        String analysisId = String.valueOf(job.getAttributes().get(Job.OPENCGA_COMMAND));
        AnalysisResult analysisResult = readAnalysisResult(analysisId, Paths.get(tmpOutdirUri));

        updateParams.setResult(analysisResult);
        updateParams.setOutput(outputFiles);

        // Check status of analysis result or if there are files that could not be moved to outdir to decide the final result
        if (analysisResult.getStatus().getName().equals(Status.Type.ERROR)) {
            updateParams.setStatus(new Job.JobStatus(Job.JobStatus.ERROR, "Job could not finish successfully"));
        } else if (allFilesMoved) {
            updateParams.setStatus(new Job.JobStatus(Job.JobStatus.DONE));
        } else {
            updateParams.setStatus(new Job.JobStatus(Job.JobStatus.UNREGISTERED, "Some files could not be moved to the final path"));
        }

        if (allFilesMoved) {
            // We want to remove the reference to the temporal file directory
            updateParams.setTmpDir(new File());
        }

        logger.info("{} - Updating job information: {}", job.getId(), updateParams.toString());
        // We update the job information
        try {
            jobManager.update(study, job.getId(), updateParams, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            logger.error("{} - Catastrophic error. Could not update job information with final result {}: {}", job.getId(),
                    updateParams.toString(), e.getMessage(), e);
            return 0;
        }

        // This has to be almost the last thing to do
        try {
            if (allFilesMoved) {
                logger.info("{} - Emptying temporal directory and deleting it from catalog", job.getId());

                catalogIOManager.deleteFile(tmpOutdirUri.resolve(ANALYSIS_RESULT_FILE));

                // Delete directory from catalog
                ObjectMap params = new ObjectMap(Constants.SKIP_TRASH, true);
                fileManager.delete(study, Collections.singletonList(job.getTmpDir().getPath()), params, token);

                // If after the file deletion it still exists, that will be because it is an external folder. In such a case, because we
                // have created the directory, we will manually delete it from the file system
                if (catalogIOManager.exists(tmpOutdirUri)) {
                    catalogIOManager.deleteDirectory(tmpOutdirUri);
                }
            } else {
                // TODO: Change temporal folder status to READY (unblock)
            }
        } catch (CatalogException e) {
            logger.error("{} - Could not clean up temporal directory: {}", job.getId(), e.getMessage(), e);
        }

        return 1;
    }

}
