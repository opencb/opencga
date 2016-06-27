package org.opencb.opencga.analysis.execution.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisOutputRecorder;
import org.opencb.opencga.analysis.execution.plugins.PluginExecutor;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.exec.Command;
import org.opencb.opencga.core.exec.RunnableProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

/**
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class LocalExecutorManager implements ExecutorManager {
    protected static Logger logger = LoggerFactory.getLogger(LocalExecutorManager.class);

    protected final CatalogManager catalogManager;
    protected final String sessionId;

    public LocalExecutorManager(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    @Override
    public QueryResult<Job> run(Job job) throws CatalogException, AnalysisExecutionException {

        String status = job.getStatus().getName();
        switch (status) {
            case Job.JobStatus.QUEUED:
            case Job.JobStatus.PREPARED:
                // change to RUNNING
                catalogManager.modifyJob(job.getId(), new ObjectMap("status.name", Job.JobStatus.RUNNING), sessionId);
                break;
            case Job.JobStatus.RUNNING:
                //nothing
                break;
            default:
                throw new AnalysisExecutionException("Unable to execute job in status " + status);
        }

        if (isPlugin(job)) {
            return runPlugin(job);
        } else {
            return runThreadLocal(job);
        }
    }

    public boolean isPlugin(Job job) {
        return Boolean.parseBoolean(Objects.toString(job.getAttributes().get("plugin")));
    }

    /**
     * Executes a job using a {@link Command}
     *
     * @param job       Job to execute
     * @return          Modified job
     * @throws CatalogException
     */
    protected QueryResult<Job> runThreadLocal(Job job) throws CatalogException, AnalysisExecutionException {

        Command com = new Command(job.getCommandLine());
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(job.getTmpOutDirUri());
        URI sout = job.getTmpOutDirUri().resolve(job.getName() + "." + job.getId() + ".out.txt");
        com.setOutputOutputStream(ioManager.createOutputStream(sout, false));
        URI serr = job.getTmpOutDirUri().resolve(job.getName() + "." + job.getId() + ".err.txt");
        com.setErrorOutputStream(ioManager.createOutputStream(serr, false));

        final long jobId = job.getId();



        Thread hook = new Thread(() -> {
            try {
                logger.info("Running ShutdownHook. Job {id: " + jobId + "} has being aborted.");
                com.setStatus(RunnableProcess.Status.KILLED);
                com.setExitValue(-2);
                postExecuteCommand(job, com, Job.ERRNO_ABORTED);
            } catch (CatalogException e) {
                e.printStackTrace();
            }
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

        return postExecuteCommand(job, com, null);
    }

    /**
     * Executes a job using the {@link PluginExecutor}
     *
     * @param job       Job to be executed
     * @return          Modified job
     * @throws AnalysisExecutionException
     * @throws CatalogException
     */
    protected QueryResult<Job> runPlugin(Job job) throws AnalysisExecutionException, CatalogException {

        PluginExecutor pluginExecutor = new PluginExecutor(catalogManager, sessionId);
        final ObjectMap executionInfo = new ObjectMap();

        final long jobId = job.getId();
        Thread hook = new Thread(() -> {
            try {
                logger.info("Running ShutdownHook. Job {id: " + jobId + "} has being aborted.");
                postExecuteLocal(job, 2, executionInfo, Job.ERRNO_ABORTED);
            } catch (CatalogException e) {
                e.printStackTrace();
            }
        });

        logger.info("==========================================");
        logger.info("Executing job {}({})", job.getName(), job.getId());
        logger.debug("Executing commandLine {}", job.getCommandLine());
        logger.info("==========================================");
        System.err.println();

        int exitValue;
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            exitValue = pluginExecutor.run(job);
        } catch (Exception e) {
            exitValue = 1;
            e.printStackTrace();
            executionInfo.put("exception", e);
        }
        Runtime.getRuntime().removeShutdownHook(hook);

        System.err.println();
        logger.info("==========================================");
        logger.info("Finished job {}({})", job.getName(), job.getId());
        logger.info("==========================================");

        return postExecuteLocal(job, exitValue, executionInfo, null);
    }

    /**
     * Closes command output and executes {@link LocalExecutorManager#postExecuteLocal(Job, int, Object, String)}
     */
    protected QueryResult<Job> postExecuteCommand(Job job, Command com, String errnoFinishError)
            throws CatalogException {
        closeOutputStreams(com);
        return postExecuteLocal(job, com.getExitValue(), com, errnoFinishError);
    }

    /**
     * Changes the job status and record output files in catalog.
     *
     *
     * @param job           Job to be post processed
     * @param exitValue     Exit value. If equals zero and no error is given, the job status will be set to READY.
     * @param executionInfo Optional execution info
     * @param error         Optional error. If this parameter not null, the job status will be set as ERROR,
     *                      even if the exitValue is 0.
     * @return              QueryResult with the modifier Job
     * @throws CatalogException
     */
    protected QueryResult<Job> postExecuteLocal(Job job, int exitValue, Object executionInfo, String error)
            throws CatalogException {

        /** Change status to DONE  - Add the execution information to the job entry **/
//                new AnalysisJobManager().jobFinish(jobQueryResult.first(), com.getExitValue(), com);
        ObjectMap parameters = new ObjectMap();
        if (executionInfo != null) {
            parameters.put("resourceManagerAttributes", new ObjectMap("executionInfo", executionInfo));
        }
        parameters.put("status.name", Job.JobStatus.DONE);
        catalogManager.modifyJob(job.getId(), parameters, sessionId);

        /** Record output **/
        AnalysisOutputRecorder outputRecorder = new AnalysisOutputRecorder(catalogManager, sessionId);
        outputRecorder.recordJobOutputAndPostProcess(job, exitValue != 0);

        /** Change status to READY or ERROR **/
        if (exitValue == 0 && StringUtils.isEmpty(error)) {
            catalogManager.modifyJob(job.getId(), new ObjectMap("status.name", Job.JobStatus.READY), sessionId);
        } else {
            if (error == null) {
                error = Job.ERRNO_FINISH_ERROR;
            }
            parameters = new ObjectMap();
            parameters.put("status.name", Job.JobStatus.ERROR);
            parameters.put("error", error);
            parameters.put("errorDescription", Job.ERROR_DESCRIPTIONS.get(error));
            catalogManager.modifyJob(job.getId(), parameters, sessionId);
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
