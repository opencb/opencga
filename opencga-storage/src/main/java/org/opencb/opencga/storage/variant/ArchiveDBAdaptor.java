package org.opencb.opencga.storage.variant;

import java.util.List;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface ArchiveDBAdaptor {
    
    QueryResult countStudies();
    
    QueryResult countStudiesPerSpecies(QueryOptions options);
    
    QueryResult countStudiesPerType(QueryOptions options);
    
    QueryResult countFiles();
    
    QueryResult countSpecies();
    
}
