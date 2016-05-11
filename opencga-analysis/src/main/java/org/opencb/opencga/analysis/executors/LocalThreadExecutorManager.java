package org.opencb.opencga.analysis.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisOutputRecorder;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.exec.Command;
import org.opencb.opencga.core.exec.RunnableProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class LocalThreadExecutorManager implements ExecutorManager {

    protected final CatalogManager catalogManager;
    protected final Logger logger = LoggerFactory.getLogger(LocalThreadExecutorManager.class);
    public LocalThreadExecutorManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public void execute(Job job, String sessionId)
            throws CatalogException, AnalysisExecutionException {

        String status = job.getStatus().getStatus();
        switch (status) {
            case Job.JobStatus.QUEUED:
            case Job.JobStatus.PREPARED:
                // change to RUNNING
                catalogManager.modifyJob(job.getId(), new ObjectMap("status.status", Job.JobStatus.RUNNING), sessionId);
                break;
            case Job.JobStatus.RUNNING:
                //nothing
                break;
            default:
                throw new AnalysisExecutionException("Unable to execute job in status " + status);
        }

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
                postExecuteLocal(job, sessionId, com);
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

        postExecuteLocal(job, sessionId, com);
    }

    protected void postExecuteLocal(Job job, String sessionId, Command com)
            throws CatalogException {
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

        ObjectMap executionInfo = new ObjectMap("executionInfo", com);
        int exitValue = com.getExitValue();
        updateStatus(job, exitValue, executionInfo, sessionId);

    }

    protected void updateStatus(Job job, int exitValue, ObjectMap executionInfo, String sessionId) throws CatalogException {
        /** Change status to DONE  - Add the execution information to the job entry **/
//                new AnalysisJobManager().jobFinish(jobQueryResult.first(), com.getExitValue(), com);
        ObjectMap parameters = new ObjectMap();
        parameters.put("resourceManagerAttributes", executionInfo);
        parameters.put("status.status", Job.JobStatus.DONE);
        catalogManager.modifyJob(job.getId(), parameters, sessionId);

        /** Record output **/
        AnalysisOutputRecorder outputRecorder = new AnalysisOutputRecorder(catalogManager, sessionId);
        outputRecorder.recordJobOutputAndPostProcess(job, exitValue != 0);

        /** Change status to READY or ERROR **/
        if (exitValue == 0) {
            catalogManager.modifyJob(job.getId(), new ObjectMap("status.status", Job.JobStatus.READY), sessionId);
        } else {
            parameters = new ObjectMap();
            parameters.put("status.status", Job.JobStatus.ERROR);
            parameters.put("error", Job.ERRNO_FINISH_ERROR);
            parameters.put("errorDescription", Job.ERROR_DESCRIPTIONS.get(Job.ERRNO_FINISH_ERROR));
            catalogManager.modifyJob(job.getId(), parameters, sessionId);
        }
    }

}
