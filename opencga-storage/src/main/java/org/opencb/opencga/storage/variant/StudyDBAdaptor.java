package org.opencb.opencga.storage.variant;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface StudyDBAdaptor {
    
    QueryResult listStudies();
    
    QueryResult countSources();
    
    QueryResult findStudyNameOrStudyId(String studyId, QueryOptions options);
    
    QueryResult getStudyById(String studyId, QueryOptions options);
    
    QueryResult getAllSourcesByStudyId(String studyId, QueryOptions options);
    
    QueryResult getSamplesBySource(String fileId, String studyId, QueryOptions options);
    
    public boolean close();

}
