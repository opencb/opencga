package org.opencb.opencga.catalog.monitor.executors.old;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.monitor.exceptions.ExecutionException;
import org.opencb.opencga.core.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/*
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public interface ExecutorManager {

    String OPENCGA_ANALYSIS_JOB_EXECUTOR = "OPENCGA.ANALYSIS.JOB.EXECUTOR";
    String EXECUTE = "execute";
    String SIMULATE = "simulate";
    String TMP_OUT_DIR = "tmpOutDir";

    // Just for test purposes. Do not use in production!
    AtomicReference<BiFunction<CatalogManager, String, ExecutorManager>> LOCAL_EXECUTOR_FACTORY = new AtomicReference<>();
    Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);

    static void execute(CatalogManager catalogManager, Job job, String sessionId)
            throws ExecutionException, IOException, CatalogException {
        // read execution param
        String defaultJobExecutor = Config.getAnalysisProperties().getProperty(OPENCGA_ANALYSIS_JOB_EXECUTOR, "LOCAL").trim().toUpperCase();
        execute(catalogManager, job, sessionId, job.getResourceManagerAttributes().getOrDefault("executor", defaultJobExecutor).toString());
    }

    static QueryResult<Job> execute(CatalogManager catalogManager, Job job, String sessionId, String jobExecutor)
            throws ExecutionException, CatalogException, IOException {
        LOGGER.debug("Execute, job: {}", job);

        final QueryResult<Job> result;
        switch (jobExecutor.toUpperCase()) {
            case "SGE":
                LOGGER.debug("Execute, running by SgeManager");
                try {
                    result = new SgeExecutorManager(catalogManager, sessionId).run(job);
                } catch (Exception e) {
                    LOGGER.error("Error executing SGE", e);
                    throw new ExecutionException("ERROR: sge execution failed.", e);
                }
                break;
            case "LOCAL":
            default:
                if (LOCAL_EXECUTOR_FACTORY.get() != null) {
                    ExecutorManager localExecutor = LOCAL_EXECUTOR_FACTORY.get().apply(catalogManager, sessionId);
                    LOGGER.debug("Execute, running by " + localExecutor.getClass());
                    try {
                        result = localExecutor.run(job);
                    } catch (Exception e) {
                        LOGGER.error("Error executing local", e);
                        throw new ExecutionException(e);
                    }
                } else {
                    LOGGER.debug("Execute, running by LocalExecutorManager");
                    result = new LocalExecutorManager(catalogManager, sessionId).run(job);
                }
                break;
        }
        return result;
    }

    @Deprecated
    QueryResult<Job> run(Job job) throws Exception;

    default String status(Job job) throws Exception {
        return job.getStatus().getName();
    }

    default QueryResult<Job> stop(Job job) throws Exception {
        throw new UnsupportedOperationException();
    }

    default QueryResult<Job> resume(Job job) throws Exception {
        throw new UnsupportedOperationException();
    }

    default QueryResult<Job> kill(Job job) throws Exception {
        throw new UnsupportedOperationException();
    }

}
