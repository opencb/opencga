package org.opencb.opencga.catalog.monitor.executors.old;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.SgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/*
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt
 */
@Deprecated
public class SgeExecutorManager implements ExecutorManager {
    protected static Logger logger = LoggerFactory.getLogger(SgeExecutorManager.class);

    private final CatalogManager catalogManager;
    private final String sessionId;

    public SgeExecutorManager(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    @Deprecated
    @Override
    public QueryResult<Job> run(Job job) throws Exception {
        // TODO: Lock job before submit. Avoid double submission
//        SgeManager.queueJob(job.getToolName(), job.getResourceManagerAttributes().get(Job.JOB_SCHEDULER_NAME).toString(),
//                -1, job.getTmpOutDirUri().getPath(), job.getCommandLine(), null, "job." + job.getId());
        return catalogManager.modifyJob(job.getId(), new ObjectMap(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Job.JobStatus.QUEUED),
                sessionId);
    }

    @Override
    public String status(Job job) throws Exception {
        String status = SgeManager.status(Objects.toString(job.getResourceManagerAttributes().get(Job.JOB_SCHEDULER_NAME)));
        switch (status) {
            case SgeManager.ERROR:
            case SgeManager.EXECUTION_ERROR:
                return Job.JobStatus.ERROR;
            case SgeManager.FINISHED:
                return Job.JobStatus.READY;
            case SgeManager.QUEUED:
                return Job.JobStatus.QUEUED;
            case SgeManager.RUNNING:
            case SgeManager.TRANSFERRED:
                return Job.JobStatus.RUNNING;
            case SgeManager.UNKNOWN:
            default:
                return job.getStatus().getName();
        }
    }
}
