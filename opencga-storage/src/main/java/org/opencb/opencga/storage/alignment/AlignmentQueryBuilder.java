package org.opencb.opencga.storage.alignment;

import java.util.List;
import org.opencb.commons.bioformats.alignment.Alignment;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.ObjectMap;
import org.opencb.commons.containers.map.QueryOptions;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface AlignmentQueryBuilder {
    
    QueryResult<Alignment> getAlignmentsByRegion(String chromosome, long start, long end, QueryOptions options);
    
    QueryResult<Alignment> getAlignmentsByGene(String gene, QueryOptions options);
    
    QueryResult<List<ObjectMap>> getAlignmentsHistogramByRegion(String chromosome, long start, long end, boolean histogramLogarithm, int histogramMax);
    
}
