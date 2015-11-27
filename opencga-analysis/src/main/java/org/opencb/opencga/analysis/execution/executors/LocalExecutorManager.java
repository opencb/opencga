package org.opencb.opencga.analysis.execution.executors;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
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
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class LocalExecutorManager implements ExecutorManager {
    protected static Logger logger = LoggerFactory.getLogger(LocalExecutorManager.class);

    private final CatalogManager catalogManager;
    private final String sessionId;

    public LocalExecutorManager(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    @Override
    public QueryResult<Job> run(Job job) throws CatalogException {
        Command com = new Command(job.getCommandLine());
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(job.getTmpOutDirUri());
        URI sout = job.getTmpOutDirUri().resolve(job.getName() + "." + job.getId() + ".out.txt");
        com.setOutputOutputStream(ioManager.createOutputStream(sout, false));
        URI serr = job.getTmpOutDirUri().resolve(job.getName() + "." + job.getId() + ".err.txt");
        com.setErrorOutputStream(ioManager.createOutputStream(serr, false));

        final int jobId = job.getId();
        Thread hook = new Thread(() -> {
            try {
                logger.info("Running ShutdownHook. Job {id: " + jobId + "} has being aborted.");
                com.setStatus(RunnableProcess.Status.KILLED);
                com.setExitValue(-2);
                postExecuteLocal(job, com);
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

        return postExecuteLocal(job, com);
    }

    private QueryResult<Job> postExecuteLocal(Job job, Command com)
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

        /** Change status to DONE  - Add the execution information to the job entry **/
//                new AnalysisJobManager().jobFinish(jobQueryResult.first(), com.getExitValue(), com);
        ObjectMap parameters = new ObjectMap();
        parameters.put("resourceManagerAttributes", new ObjectMap("executionInfo", com));
        parameters.put("status", Job.Status.DONE);
        catalogManager.modifyJob(job.getId(), parameters, sessionId);

        /** Record output **/
        AnalysisOutputRecorder outputRecorder = new AnalysisOutputRecorder(catalogManager, sessionId);
        outputRecorder.recordJobOutputAndPostProcess(job, com.getExitValue() != 0);

        /** Change status to READY or ERROR **/
        if (com.getExitValue() == 0) {
            catalogManager.modifyJob(job.getId(), new ObjectMap("status", Job.Status.READY), sessionId);
        } else {
            parameters = new ObjectMap();
            parameters.put("status", Job.Status.ERROR);
            parameters.put("error", Job.ERRNO_FINISH_ERROR);
            parameters.put("errorDescription", Job.errorDescriptions.get(Job.ERRNO_FINISH_ERROR));
            catalogManager.modifyJob(job.getId(), parameters, sessionId);
        }

        return catalogManager.getJob(job.getId(), new QueryOptions(), sessionId);
    }

}
