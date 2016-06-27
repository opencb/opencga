package org.opencb.opencga.analysis.execution.executors;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ExecutorManager {

    String OPENCGA_ANALYSIS_JOB_EXECUTOR = "OPENCGA.ANALYSIS.JOB.EXECUTOR";
    String EXECUTE = "execute";
    String SIMULATE = "simulate";

    // Just for test purposes. Do not use in production!
    AtomicReference<BiFunction<CatalogManager, String, ExecutorManager>> localExecutorFactory = new AtomicReference<>();
    Logger logger = LoggerFactory.getLogger(ExecutorManager.class);

    static void execute(CatalogManager catalogManager, Job job, String sessionId)
            throws AnalysisExecutionException, IOException, CatalogException {
        // read execution param
        String defaultJobExecutor = Config.getAnalysisProperties().getProperty(OPENCGA_ANALYSIS_JOB_EXECUTOR, "LOCAL").trim().toUpperCase();
        execute(catalogManager, job, sessionId, job.getResourceManagerAttributes().getOrDefault("executor", defaultJobExecutor).toString());
    }

    static QueryResult<Job> execute(CatalogManager catalogManager, Job job, String sessionId, String jobExecutor)
            throws AnalysisExecutionException, CatalogException {
        logger.debug("Execute, job: {}", job);

        final QueryResult<Job> result;
        switch (jobExecutor.toUpperCase()) {
            default:
            case "LOCAL":
                if (localExecutorFactory.get() != null) {
                    ExecutorManager localExecutor = localExecutorFactory.get().apply(catalogManager, sessionId);
                    logger.debug("Execute, running by " + localExecutor.getClass());
                    try {
                        result = localExecutor.run(job);
                    } catch (Exception e) {
                        logger.error("Error executing local", e);
                        throw new AnalysisExecutionException(e);
                    }
                } else {
                    logger.debug("Execute, running by LocalExecutorManager");
                    result = new LocalExecutorManager(catalogManager, sessionId).run(job);
                }
                break;
            case "SGE":
                logger.debug("Execute, running by SgeManager");
                try {
                    result = new SgeExecutorManager(catalogManager, sessionId).run(job);
                } catch (Exception e) {
                    logger.error("Error executing SGE", e);
                    throw new AnalysisExecutionException("ERROR: sge execution failed.", e);
                }
                break;
        }
        return result;
    }

    QueryResult<Job> run(Job job) throws Exception;

    default String status(Job job) throws Exception {
        return job.getStatus().getName();
    }

    default QueryResult<Job> stop(Job job) throws Exception { throw new UnsupportedOperationException(); }

    default QueryResult<Job> resume(Job job) throws Exception { throw new UnsupportedOperationException(); }

    default QueryResult<Job> kill(Job job) throws Exception { throw new UnsupportedOperationException(); }

}
