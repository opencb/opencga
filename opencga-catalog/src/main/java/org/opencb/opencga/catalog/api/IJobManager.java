package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.Job;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface IJobManager extends ResourceManager<Integer, Job> {
    public Integer getStudyId(int jobId);

    public QueryResult<Job> visit(int jobId);
}
