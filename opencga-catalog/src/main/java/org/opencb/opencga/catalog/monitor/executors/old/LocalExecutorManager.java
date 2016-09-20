package org.opencb.opencga.catalog.monitor.executors.old;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.exec.Command;
import org.opencb.commons.exec.RunnableProcess;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.monitor.exceptions.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import static org.opencb.opencga.catalog.monitor.executors.AbstractExecutor.JOB_STATUS_FILE;

/*
 * Created on 26/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public class LocalExecutorManager implements ExecutorManager {
    protected static Logger logger = LoggerFactory.getLogger(LocalExecutorManager.class);

    protected final CatalogManager catalogManager;
    protected final String sessionId;

    public LocalExecutorManager(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    @Override
    public QueryResult<Job> run(Job job) throws CatalogException, ExecutionException, IOException {

//        String status = job.getStatus().getName();
//        switch (status) {
//            case Job.JobStatus.QUEUED:
//            case Job.JobStatus.PREPARED:
//                // change to RUNNING
//                catalogManager.modifyJob(job.getId(),
//                        new ObjectMap(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.RUNNING), sessionId);
//                break;
//            case Job.JobStatus.RUNNING:
//            default:
//                throw new ExecutionException("Unable to execute job in status " + status);
//        }

//        if (isPlugin(job)) {
//            return runPlugin(job);
//        } else {
//            return runThreadLocal(job);
//    }
        return runThreadLocal(job);
    }

    public boolean isPlugin(Job job) {
        return Boolean.parseBoolean(Objects.toString(job.getAttributes().get("plugin")));
    }

    /**
     * Executes a job using a {@link Command}.
     *
     * @param job       Job to execute.
     * @return          Modified job.
     * @throws CatalogException catalogException.
     * @throws ExecutionException executionException.
     * @throws IOException ioExeption.
     */
    protected QueryResult<Job> runThreadLocal(Job job) throws CatalogException, ExecutionException, IOException {
        logger.info("Ready to run {}", job.getCommandLine());
        Command com = new Command(job.getCommandLine());

//        URI tmpOutDir = Paths.get((String) job.getAttributes().get(TMP_OUT_DIR)).toUri();
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get("file");
//        URI sout = tmpOutDir.resolve(job.getName() + "." + job.getId() + ".out.txt");
//        com.setOutputOutputStream(ioManager.createOutputStream(sout, false));
//        URI serr = tmpOutDir.resolve(job.getName() + "." + job.getId() + ".err.txt");
//        com.setErrorOutputStream(ioManager.createOutputStream(serr, false));

        if (job.getResourceManagerAttributes().containsKey("STDOUT")) {
            URI sout = Paths.get((String) job.getResourceManagerAttributes().get("STDOUT")).toUri();
            com.setOutputOutputStream(ioManager.createOutputStream(sout, false));
        }
        if (job.getResourceManagerAttributes().containsKey("STDERR")) {
            URI serr = Paths.get((String) job.getResourceManagerAttributes().get("STDERR")).toUri();
            com.setErrorOutputStream(ioManager.createOutputStream(serr, false));
        }

        final long jobId = job.getId();

        Thread hook = new Thread(() -> {
//            try {
            logger.info("Running ShutdownHook. Job {id: " + jobId + "} has being aborted.");
            com.setStatus(RunnableProcess.Status.KILLED);
            com.setExitValue(-2);
            closeOutputStreams(com);
//                postExecuteCommand(job, com, Job.ERRNO_ABORTED);
//            } catch (CatalogException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        });

        logger.info("==========================================");
        logger.info("Executing job {}({})", job.getName(), job.getId());
        logger.debug("Executing commandLine {}", job.getCommandLine());
        logger.info("==========================================");
        System.err.println();

        Runtime.getRuntime().addShutdownHook(hook);
        com.run();
        Runtime.getRuntime().removeShutdownHook(hook);

        System.err.println();
        logger.info("==========================================");
        logger.info("Finished job {}({})", job.getName(), job.getId());
        logger.info("==========================================");

        closeOutputStreams(com);
        return catalogManager.getJobManager().get(job.getId(), new QueryOptions(), sessionId);
//        return postExecuteCommand(job, com, null);
    }

//    /**
//     * Executes a job using the {@link PluginExecutor}
//     *
//     * @param job       Job to be executed
//     * @return          Modified job
//     * @throws ExecutionException
//     * @throws CatalogException
//     */
//    protected QueryResult<Job> runPlugin(Job job) throws ExecutionException, CatalogException {
//
//        PluginExecutor pluginExecutor = new PluginExecutor(catalogManager, sessionId);
//        final ObjectMap executionInfo = new ObjectMap();
//
//        final long jobId = job.getId();
//        Thread hook = new Thread(() -> {
//            try {
//                logger.info("Running ShutdownHook. Job {id: " + jobId + "} has being aborted.");
//                postExecuteLocal(job, 2, executionInfo, Job.ERRNO_ABORTED);
//            } catch (CatalogException e) {
//                e.printStackTrace();
//            }
//        });
//
//        logger.info("==========================================");
//        logger.info("Executing job {}({})", job.getName(), job.getId());
//        logger.debug("Executing commandLine {}", job.getCommandLine());
//        logger.info("==========================================");
//        System.err.println();
//
//        int exitValue;
//        Runtime.getRuntime().addShutdownHook(hook);
//        try {
//            exitValue = pluginExecutor.run(job);
//        } catch (Exception e) {
//            exitValue = 1;
//            e.printStackTrace();
//            executionInfo.put("exception", e);
//        }
//        Runtime.getRuntime().removeShutdownHook(hook);
//
//        System.err.println();
//        logger.info("==========================================");
//        logger.info("Finished job {}({})", job.getName(), job.getId());
//        logger.info("==========================================");
//
//        return postExecuteLocal(job, exitValue, executionInfo, null);
//    }

    /**
     * Closes command output and executes {@link LocalExecutorManager#postExecuteLocal(Job, int, Object, String)}.
     * @param job job.
     * @param com com.
     * @param errnoFinishError errnoFinishError.
     * @return queryResult.
     * @throws CatalogException catalogException.
     * @throws IOException ioExeption.
     */
    @Deprecated
    protected QueryResult<Job> postExecuteCommand(Job job, Command com, String errnoFinishError) throws CatalogException, IOException {
        closeOutputStreams(com);
        return postExecuteLocal(job, com.getExitValue(), com, errnoFinishError);
    }

    /**
     * Changes the job status and record output files in catalog.
     *
     *
     * @param job           Job to be post processed.
     * @param exitValue     Exit value. If equals zero and no error is given, the job status will be set to READY.
     * @param executionInfo Optional execution info.
     * @param error         Optional error. If this parameter not null, the job status will be set as ERROR.
     *                      even if the exitValue is 0.
     * @return              QueryResult with the modifier Job.
     * @throws CatalogException catalogException.
     * @throws IOException ioExeption.
     */
    @Deprecated
    protected QueryResult<Job> postExecuteLocal(Job job, int exitValue, Object executionInfo, String error)
            throws CatalogException, IOException {

        /** Change status to DONE  - Add the execution information to the job entry **/
//                new JobManager().jobFinish(jobQueryResult.first(), com.getExitValue(), com);
        ObjectMap parameters = new ObjectMap();
        if (executionInfo != null) {
            parameters.put("resourceManagerAttributes", new ObjectMap("executionInfo", executionInfo));
        }
//        parameters.put(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.DONE);
        catalogManager.modifyJob(job.getId(), parameters, sessionId);

//        /** Record output **/
//        ExecutionOutputRecorder outputRecorder = new ExecutionOutputRecorder(catalogManager, sessionId);
//        outputRecorder.recordJobOutputAndPostProcess(job, exitValue != 0);

        ObjectMapper objectMapper = new ObjectMapper();
        Path outdir = Paths.get((String) job.getAttributes().get(TMP_OUT_DIR));
        Job.JobStatus status = objectMapper.reader(Job.JobStatus.class).readValue(outdir.resolve(JOB_STATUS_FILE).toFile());
        /** Change status to READY or ERROR **/
        if (exitValue == 0 && StringUtils.isEmpty(error)) {
            objectMapper.writer().writeValue(outdir.resolve(JOB_STATUS_FILE).toFile(), new Job.JobStatus(Job.JobStatus.DONE,
                    "Job finished."));
//            catalogManager.modifyJob(job.getId(), new ObjectMap(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.READY),
//                    sessionId);
        } else {
            if (error == null) {
                error = Job.ERRNO_FINISH_ERROR;
            }
            parameters = new ObjectMap();
//            parameters.put(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.ERROR);
            parameters.put(JobDBAdaptor.QueryParams.ERROR.key(), error);
            parameters.put(JobDBAdaptor.QueryParams.ERROR_DESCRIPTION.key(), Job.ERROR_DESCRIPTIONS.get(error));
            catalogManager.modifyJob(job.getId(), parameters, sessionId);
            objectMapper.writer().writeValue(outdir.resolve(JOB_STATUS_FILE).toFile(), new Job.JobStatus(Job.JobStatus.ERROR,
                    "Job finished with error."));
        }

        return catalogManager.getJob(job.getId(), new QueryOptions(), sessionId);
    }

    protected void closeOutputStreams(Command com) {
        /** Close output streams **/
        if (com.getOutputOutputStream() != null) {
            try {
                com.getOutputOutputStream().close();
            } catch (IOException e) {
                logger.warn("Error closing OutputStream", e);
            }
            com.setOutputOutputStream(null);
            com.setOutput(null);
        }
        if (com.getErrorOutputStream() != null) {
            try {
                com.getErrorOutputStream().close();
            } catch (IOException e) {
                logger.warn("Error closing OutputStream", e);
            }
            com.setErrorOutputStream(null);
            com.setError(null);
        }
    }

}
