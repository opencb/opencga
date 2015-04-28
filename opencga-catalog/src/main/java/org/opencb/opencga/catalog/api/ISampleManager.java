package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.Annotation;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Project;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface ISampleManager extends ResourceManager<Project, Integer> {
    public Integer getProjectId(String projectId);

    public QueryResult<Annotation> annotate(int sampleId);
    public QueryResult<Annotation> load(File file);

}
