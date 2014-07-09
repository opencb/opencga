package org.opencb.opencga.storage.variant;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface StudyDBAdaptor {
    
    QueryResult listStudies();
    
    QueryResult findStudyNameOrStudyId(String studyId, QueryOptions options);
    
    QueryResult getStudyById(String studyId, QueryOptions options);
    
    boolean close();

}
