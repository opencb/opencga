package org.opencb.opencga.storage.variant;

import org.opencb.datastore.core.QueryResult;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface ArchiveDBAdaptor {
    
    QueryResult countStudies();
    
    QueryResult countFiles();
    
    QueryResult countSpecies();
    
}
