package org.opencb.opencga.analysis.execution.executors;

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.ToolManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ExecutorManager {

    static void execute(CatalogManager catalogManager, Job job, String sessionId)
            throws AnalysisExecutionException, IOException, CatalogException {
        Logger logger = LoggerFactory.getLogger(ExecutorManager.class);
        logger.debug("Execute, job: {}", job);

        // read execution param
        String defaultJobExecutor = Config.getAnalysisProperties().getProperty(ToolManager.OPENCGA_ANALYSIS_JOB_EXECUTOR, "LOCAL").trim().toUpperCase();
        String jobExecutor = job.getResourceManagerAttributes().getOrDefault("executor", defaultJobExecutor).toString();

        switch (jobExecutor) {
            default:
            case "LOCAL":
                logger.debug("Execute, running by LocalExecutorManager");
                new LocalExecutorManager(catalogManager, sessionId).run(job);
                break;
            case "SGE":
                logger.debug("Execute, running by SgeManager");
                try {
                    new SgeExecutorManager(catalogManager, sessionId).run(job);
                } catch (Exception e) {
                    logger.error(e.toString());
                    throw new AnalysisExecutionException("ERROR: sge execution failed.");
                }
                break;
        }
    }

    QueryResult<Job> run(Job job) throws Exception;

    default Job.Status status(Job job) throws Exception {
        return job.getStatus();
    }

    default QueryResult<Job> stop(Job job) throws Exception { throw new UnsupportedOperationException(); }

    default QueryResult<Job> resume(Job job) throws Exception { throw new UnsupportedOperationException(); }

    default QueryResult<Job> kill(Job job) throws Exception { throw new UnsupportedOperationException(); }

}
