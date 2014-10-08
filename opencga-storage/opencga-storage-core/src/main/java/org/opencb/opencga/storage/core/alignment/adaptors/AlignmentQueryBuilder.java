package org.opencb.opencga.storage.core.alignment.adaptors;

import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface AlignmentQueryBuilder {
    
    QueryResult getAllAlignmentsByRegion(Region region, QueryOptions options);

//    List<QueryResult> getAllAlignmentsByRegionList(List<Region> region, QueryOptions options);
    
    
//    QueryResult getAllAlignmentBlocksByRegion(Region region, QueryOptions options);
//    
//    List<QueryResult> getAllAlignmentBlocksByRegionList(List<Region> region, QueryOptions options);
    
    QueryResult getAllAlignmentsByGene(String gene, QueryOptions options);
    
    QueryResult getCoverageByRegion(Region region, QueryOptions options);
    
    QueryResult getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax);
    
    QueryResult getAlignmentRegionInfo(Region region, QueryOptions options);
    
}
