package org.opencb.opencga.analysis.execution.executors;

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.Job;

/**
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ExecutorManager {

    QueryResult<Job> run(Job job) throws Exception;

    default Job.Status status(Job job) throws Exception {
        return job.getStatus();
    }

    default QueryResult<Job> stop(Job job) throws Exception { throw new UnsupportedOperationException(); }

    default QueryResult<Job> resume(Job job) throws Exception { throw new UnsupportedOperationException(); }

    default QueryResult<Job> kill(Job job) throws Exception { throw new UnsupportedOperationException(); }

}
