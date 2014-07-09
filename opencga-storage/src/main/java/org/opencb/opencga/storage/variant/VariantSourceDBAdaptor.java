package org.opencb.opencga.storage.variant;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface VariantSourceDBAdaptor {
    
    QueryResult countSources();
    
    QueryResult getAllSources(QueryOptions options);
    
    QueryResult getAllSourcesByStudyId(String studyId, QueryOptions options);
    
    QueryResult getSamplesBySource(String fileId, String studyId, QueryOptions options);

    QueryResult getSourceDownloadUrlByName(String filename);
    
    QueryResult getSourceDownloadUrlById(String fileId, String studyId);
    
    boolean close();
    
}
