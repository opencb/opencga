package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.Annotation;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Project;
import org.opencb.opencga.catalog.beans.Sample;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface ISampleManager extends ResourceManager<Integer, Sample> {
    public Integer getProjectId(String projectId);

    public QueryResult<Annotation> annotate(int sampleId);
    public QueryResult<Annotation> load(File file);

}
